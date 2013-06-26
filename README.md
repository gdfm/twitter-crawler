twitter-crawler
===============

Crawler for the social network of Twitter.

To launch the crawler, compile it.

```bash
mvn package
```

Configure the environment with your Twitter API keys for Twitter4j.
```bash
export JAVA_OPTS="-Dtwitter4j.oauth.consumerKey=XXXXXXXXXXXXXXXXXXXXXX -Dtwitter4j.oauth.consumerSecret=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX -Dtwitter4j.oauth.accessToken=XXXXXXXXX-XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX -Dtwitter4j.oauth.accessTokenSecret=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
```

Then launch it.

```bash
sh target/appassembler/bin/crawl input.txt output.csv
```

The input.txt file should contain one user ID per line.
