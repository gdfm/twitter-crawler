package com.yahoo.research.bcn;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import twitter4j.IDs;
import twitter4j.ResponseList;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.User;

public class Crawler {
    private Twitter twitter = Utils.getTwitterInstance();
    private BufferedReader reader;
    private PrintWriter writer;

    private Map<Integer, Integer> errorMap = new HashMap<Integer, Integer>();
    private static final int CHUNK_SIZE = 100;
    private static final int MAX_RETRIES = 5;

    // state variables
    private String currentUsername;
    private String nextUsername = ""; // initialized for the first call to hasNextUser()
    private long[] currentFriends;
    private int chunkCursor;
    private boolean retry;
    private int consecutiveErrors;

    public Crawler(String infile, String outfile) throws IOException {
        this.reader = new BufferedReader(new FileReader(infile));
        this.writer = new PrintWriter(new BufferedWriter(new FileWriter(outfile)));
        this.nextUser();
    }

    public boolean hasNextUser() throws IOException {
        return nextUsername != null;
    }

    public void nextUser() throws IOException {
        this.currentUsername = this.nextUsername;
        this.nextUsername = this.reader.readLine();
        currentFriends = null;
    }

    public String getCurrentUsername() {
        return currentUsername;
    }

    public boolean crawlCurrentFriends() {
        try {
            IDs ids = twitter.getFriendsIDs(getCurrentUsername(), -1);
            this.currentFriends = ids.getIDs();
            this.chunkCursor = 0;
        } catch (TwitterException e) {
            setRetry(!isDone(e));
            return false;
        }
        return true;
    }

    public boolean crawlNextChunk() {
        // request screen names 100 at a time
        int chunkEnd = Math.min(chunkCursor + CHUNK_SIZE, currentFriends.length);
        long[] chunk = Arrays.copyOfRange(currentFriends, chunkCursor, chunkEnd);
        ResponseList<User> rl;
        try {
            rl = twitter.lookupUsers(chunk);
        } catch (TwitterException e) {
            setRetry(!isDone(e));
            return false;
        }
        for (Iterator<User> it = rl.iterator(); it.hasNext();) {
            User friend = it.next();
            String friendName = friend.getScreenName();
            writer.println(getCurrentUsername() + '\t' + friendName);
        }
        // move the cursor to the next chunk
        chunkCursor += CHUNK_SIZE;
        return true;
    }
    
    public boolean hasChunks() {
        return chunkCursor < currentFriends.length;
    }

    public void setRetry(boolean retry) {
        this.retry = retry;
        if (!retry)
            this.consecutiveErrors = 0;
    }

    public boolean retry() {
        return retry;
    }

    public void close() {
        writer.close();
        System.out.println("Error summary:\n" + errorMap);
    }

    public int getCurrentFriendsCount() {
        return currentFriends.length;
    }

    public int getCursor() {
        return chunkCursor;
    }

    // true = done ; false = retry
    private boolean isDone(TwitterException e) {
        // print exception
        // e.printStackTrace();
        System.err.println(e.getMessage());
        // record it for statistics
        int code = e.getStatusCode();
        if (!errorMap.containsKey(code))
            errorMap.put(code, 0);
        errorMap.put(code, errorMap.get(code) + 1);
        // if it is caused by rate limit, wait and signal it is not done
        if (e.exceededRateLimitation()) {
            if (consecutiveErrors++ > MAX_RETRIES)
                return true; // already tried enough
            int secondsToSleep = e.getRetryAfter() + 10; // 10s slack
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
            return false;
        }
        return true;
    }

    public static void main(String[] args) throws IOException {

        if (args.length < 2) {
            System.err.println("Usage:Crawler <input_file> <output_file>");
            System.exit(1);
        }

        String infile = args[0]; // "res/twitter_usernames.txt"
        String outfile = args[1]; // "res/twitter_follows.txt"
        Crawler crawler = new Crawler(infile, outfile);
        try {
            while (crawler.hasNextUser()) {
                crawler.nextUser();
                boolean ok = true;
                do {
                    if (!ok)
                        System.out.println("Retrying crawling friends of " + crawler.getCurrentUsername());
                    ok = crawler.crawlCurrentFriends();
                } while (crawler.retry());
                if (ok) {
                    System.out.println(crawler.getCurrentUsername() + " # of friends: "
                            + crawler.getCurrentFriendsCount());
                    ok = true;
                    do {
                        if (!ok)
                            System.out.println("Retrying crawling friend chunk " + crawler.getCursor() + "/"
                                    + crawler.getCurrentFriendsCount() / Crawler.CHUNK_SIZE + " of "
                                    + crawler.getCurrentUsername());

                        ok = crawler.crawlNextChunk();
                    } while (crawler.hasChunks() && crawler.retry());
                } else {
                    System.out.println("Could not access friends of " + crawler.getCurrentUsername());
                }
            }
        } finally {
            crawler.close();
        }
    }
}
