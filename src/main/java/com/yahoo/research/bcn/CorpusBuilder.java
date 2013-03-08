package com.yahoo.research.bcn;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Paging;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.internal.http.HttpResponseCode;

public class CorpusBuilder {
    private static Logger LOG = LoggerFactory.getLogger(CorpusBuilder.class);
    private static final int PAGE_SIZE = 100;
    private static final Date INIT_DATE = getInitDate();
    private static final Date FINAL_DATE = getFinalDate();
    private static final long SLEEP_TIME_MILLIS = 5000;
    private static final int MAX_RETRIES = 5;
    private Twitter twitter = Utils.getTwitterInstance();
    private File corpusDir;
    private PrintWriter corpus;
    private List<String> usernames;

    public CorpusBuilder(File inputlist, File outputdir) throws IOException {
        usernames = new LinkedList<String>();
        String user;
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(inputlist));
            while ((user = reader.readLine()) != null)
                usernames.add(user);
            corpusDir = outputdir;
        } finally {
            if (reader != null)
                reader.close();
        }
        if (LOG.isInfoEnabled())
            LOG.info("Read " + usernames.size() + " users from input file");
    }

    private static Date getInitDate() {
        Calendar c = Calendar.getInstance();
        c.set(2011, 4, 1, 0, 0, 0); // 1 May 2011 00:00:00
        return c.getTime();
    }

    private static Date getFinalDate() {
        Calendar c = Calendar.getInstance();
        c.set(2011, 4, 31, 23, 59, 59); // 31 May 2011 23:59:59
        return c.getTime();
    }

    public void run() throws InterruptedException, IOException {
        int cursor = 0, listSize = usernames.size();
        for (String username : usernames) {
            boolean success = false;
            try {
                cursor++;
                if (LOG.isInfoEnabled())
                    LOG.info("Building corpus of user (" + cursor + "/" + listSize + "): " + username);
                corpus = new PrintWriter(new BufferedWriter(new FileWriter(new File(corpusDir, username))));
                int currentTrial = 0;
                while (!success && currentTrial++ < MAX_RETRIES) {
                    try {
                        if (LOG.isInfoEnabled())
                            LOG.info("Try (" + currentTrial + "/" + MAX_RETRIES + "): " + username);
                        int startPage = findStartingPage(username);
                        success = crawl(username, startPage);
                    } catch (TwitterException e) {
                        e.printStackTrace();
                        if (e.isCausedByNetworkIssue())
                            Thread.sleep(SLEEP_TIME_MILLIS);
                        else if (e.exceededRateLimitation()) {
                            int millisToSleep = 1000 * (e.getRetryAfter() + 10); // 10s slack
                            if (LOG.isInfoEnabled())
                                LOG.info("Sleeping for " + millisToSleep / 1000 + " seconds");
                            long before = System.currentTimeMillis();
                            try {
                                Thread.sleep(millisToSleep);
                            } catch (InterruptedException ie) {
                                ie.printStackTrace();
                            }
                            long now = System.currentTimeMillis();
                            if (LOG.isInfoEnabled())
                                LOG.info("Woke up! Slept for " + (now - before) / 1000 + " seconds");
                        } else if (e.resourceNotFound() || e.getStatusCode() == HttpResponseCode.FORBIDDEN
                                || e.getStatusCode() == HttpResponseCode.UNAUTHORIZED)
                            break;
                    }
                }
            } finally {
                if (corpus != null) {
                    corpus.close();
                    if (LOG.isInfoEnabled())
                        LOG.info("Closing corpus file: " + username);
                }
                if (!success) {
                    if (LOG.isWarnEnabled())
                        LOG.warn("Could not crawl user: " + username);
                } else if (LOG.isInfoEnabled())
                    LOG.info("Crawled corpus of user: " + username);
            }
        }
        if (LOG.isInfoEnabled())
            LOG.info("End!");
    }

    private int findStartingPage(String username) throws TwitterException {
        Status status = null;
        int currentPage = 1, lastPage = 0;
        boolean found = false, close = false;
        if (LOG.isInfoEnabled())
            LOG.info("Doubling Phase: Start!");
        while (!found) {
            Paging paging = new Paging(currentPage * PAGE_SIZE, 1); // get only the first status instead of the full page
            ResponseList<Status> list = twitter.getUserTimeline(username, paging);
            if (LOG.isDebugEnabled())
                LOG.debug("Doubling Phase: got " + list.size() + " tweets");
            if (!list.isEmpty())
                status = list.iterator().next();
            found = list.isEmpty() || status.getCreatedAt().before(FINAL_DATE); // OR shortcut prevents NPE
            if (!found) {
                currentPage *= 2;
                if (LOG.isDebugEnabled())
                    LOG.debug("Doubling Phase: doubling, going to page " + currentPage);
            } else {
                if (LOG.isDebugEnabled())
                    LOG.debug("Doubling Phase: found end point! page " + currentPage);
                if (!list.isEmpty())
                    close = status.getCreatedAt().after(INIT_DATE);
                lastPage = currentPage / 2;
            }
        }

        if (LOG.isInfoEnabled())
            LOG.info("Closing Phase: Start!");
        found = currentPage <= 1;
        while (!found) {
            int currentGap = (currentPage - lastPage);
            close |= currentGap <= 1;
            if (LOG.isDebugEnabled())
                LOG.debug("CurrentGap=" + currentGap);
            if (close)
                currentPage--; // linear
            else
                currentPage -= currentGap / 2; // binary search
            if (LOG.isDebugEnabled())
                LOG.debug("Closing Phase: " + (close ? "linearly" : "binarily") + " reducing currentPage=" + currentPage);

            Paging paging = new Paging(currentPage * PAGE_SIZE, 1); // get only the first status instead of the full page
            ResponseList<Status> list = twitter.getUserTimeline(username, paging);
            if (LOG.isDebugEnabled())
                LOG.debug("Closing Phase: got " + list.size() + " tweets");
            if (!list.isEmpty()) {
                status = list.iterator().next();
                found = status.getCreatedAt().after(FINAL_DATE);
                close = status.getCreatedAt().after(INIT_DATE);
            }
            found |= currentPage <= 1;
        }
        if (found)
            if (LOG.isDebugEnabled())
                LOG.debug("Closing Phase: found end point! page " + currentPage);
        return Math.max(1, currentPage);
    }

    private boolean crawl(String username, int currentPage) throws TwitterException, InterruptedException {
        boolean done = false;
        if (LOG.isInfoEnabled())
            LOG.info("Crawling Phase: Start!");
        while (!done) {
            Paging paging = new Paging(currentPage++, PAGE_SIZE);
            ResponseList<Status> list = twitter.getUserTimeline(username, paging);
            if (LOG.isInfoEnabled())
                LOG.info("Crawling Phase: got " + list.size() + " tweets");
            for (Status s : list) {
                // System.out.println(formatFull(s));
                corpus.println(formatFull(s));
                done = s.getCreatedAt().before(INIT_DATE);
            }
            done |= list.isEmpty();
            // list.getFeatureSpecificRateLimitStatus();
            Thread.sleep(SLEEP_TIME_MILLIS);
        }
        return true;
    }

    public static String formatFull(Status s) {
        String sep = "\t";
        return s.getId() + sep + s.getUser().getScreenName() + sep + s.getCreatedAt() + sep + s.getText();
    }

    public static void main(String[] args) throws TwitterException, IOException, InterruptedException {
        if (args.length < 2) {
            System.err.println("Usage: " + CorpusBuilder.class.getName() + " <users_list_file> <output_directory>");
            System.exit(1);
        }
        new CorpusBuilder(new File(args[0]), new File(args[1])).run();
    }
}
