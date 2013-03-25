package com.yahoo.research.bcn;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import twitter4j.CursorSupport;
import twitter4j.IDs;
import twitter4j.Twitter;
import twitter4j.TwitterException;

public class TwitterCrawler {

    public static void main(String[] args) throws IOException {

        if (args.length < 2) {
            System.err.println("Usage: Crawler <input_file> <output_file>");
            System.exit(1);
        }

        String infile = args[0];
        String outfile = args[1];
        BufferedReader in = new BufferedReader(new FileReader(infile));
        PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outfile)));
        TwitterCrawler crawler = new TwitterCrawler(in, out);
        try {
            while (crawler.hasNextUser()) {
                crawler.nextUser();
                System.out.println("Crawling friends of user " + crawler.getCurrentUserID());
                boolean done;
                do {
                    done = false;
                    while (!done && crawler.retry()) {
                        try {
                            done = crawler.crawlNextFriends();
                        } catch (TwitterException e) {
                            crawler.handleTwitterException(e);
                        }
                    }
                } while (crawler.hasMorePages() && crawler.retry());
                if (!done) {
                    System.out.println("Could not access friends of " + crawler.getCurrentUserID());
                }
            }
        } finally {
            crawler.close();
        }
    }

    private Twitter twitter = Utils.getTwitterInstance();
    private BufferedReader reader;
    private PrintWriter writer;
    private Map<Integer, Integer> errorMap = new HashMap<Integer, Integer>();

    private static final int MAX_RETRIES = 5;
    // state variables
    private long currentUserID;
    private long nextUserID = 0; // initialized for the first call to hasNextUser()
    private long currentCursor;
    private IDs currentFriends;
    private int consecutiveErrors;
    private boolean retry;

    public TwitterCrawler(BufferedReader in, PrintWriter out) throws IOException {
        this.reader = in;
        this.writer = out;
        this.nextUser(); // load first user
    }

    public void close() {
        writer.close();
        System.err.println("Error summary:\n" + errorMap);
    }

    // true = ok, false = exception
    public boolean crawlNextFriends() throws TwitterException {
        this.currentFriends = twitter.getFriendsIDs(getCurrentUserID(), currentCursor);
        for (long friendID : currentFriends.getIDs())
            writer.println(String.format("%d\t%d", getCurrentUserID(), friendID));
        writer.flush();
        if (currentFriends.hasNext())
            currentCursor = currentFriends.getNextCursor();
        return true;
    }

    public long getCurrentUserID() {
        return currentUserID;
    }

    private void handleTwitterException(TwitterException e) {
        if (e.exceededRateLimitation()) {
            logException(e);
            int secondsToSleep = e.getRateLimitStatus().getSecondsUntilReset() + 1; // 1s slack
            int millisToSleep = 1000 * secondsToSleep;
            System.out.println("[" + new Date() + "] Sleeping for " + secondsToSleep + " seconds");
            long before = System.currentTimeMillis();
            try {
                Thread.sleep(millisToSleep);
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
            long now = System.currentTimeMillis();
            System.out.println("[" + new Date() + "] Woke up! Slept for " + (now - before) / 1000 + " seconds");
        } else {
            logException(e);
            if (consecutiveErrors++ > MAX_RETRIES)
                setRetry(false); // already tried enough
        }
    }

    public boolean hasMorePages() {
        return (currentFriends != null && currentFriends.hasNext());
    }

    public boolean hasNextUser() throws IOException {
        return nextUserID >= 0;
    }

    private void logException(TwitterException e) {
        // print exception
        System.err.println(e.getMessage());
        // record it for statistics
        int code = e.getStatusCode();
        if (!errorMap.containsKey(code))
            errorMap.put(code, 0);
        errorMap.put(code, errorMap.get(code) + 1);
    }

    public void nextUser() throws IOException {
        currentUserID = nextUserID;
        String line = reader.readLine();
        if (line == null)
            nextUserID = -1;
        else
            nextUserID = Long.parseLong(line);
        currentFriends = null;
        currentCursor = CursorSupport.START;
        resetRetry();
        writer.flush();
    }

    private boolean retry() {
        return retry;
    }

    private void setRetry(boolean retry) {
        this.retry = retry;
    }

    private void resetRetry() {
        consecutiveErrors = 0; // reset retry counter
        retry = true;
    }

}
