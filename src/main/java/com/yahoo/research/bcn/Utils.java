package com.yahoo.research.bcn;

import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class Utils {

    public static Twitter getTwitterInstance() {
        ConfigurationBuilder cb = new ConfigurationBuilder().setDebugEnabled(true);
        // .setOAuthConsumerKey("your-key")
        // .setOAuthConsumerSecret("your-secret")
        // .setOAuthAccessToken("your-token")
        // .setOAuthAccessTokenSecret("your-secret-token");
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();
        return twitter;
    }
}
