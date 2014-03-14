package com.yahoo.research.bcn;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;

import org.junit.Test;

import twitter4j.TwitterException;

public class TwitterCrawlerTest {
    private static final String USER_ID = "234017215";

//    @Test
    public void test() throws IOException, TwitterException {
        StringWriter sw = new StringWriter();
        BufferedReader in = new BufferedReader(new StringReader(USER_ID));
        PrintWriter out = new PrintWriter(sw);
        TwitterCrawler crawler = new TwitterCrawler(in, out);
        crawler.nextUser();
        crawler.crawlNextFriends();
        crawler.close();
        String results = sw.toString();
        assertTrue("No results", results.length() > 0);
        System.out.println(results);
    }
}
