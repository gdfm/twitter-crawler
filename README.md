twitter-crawler
===============

Crawler for the social network of Twitter.

To launch the crawler, compile it.

```bash
mvn package
```
Then launch it.

```bash
sh target/appassembler/bin/crawl input.txt output.csv
```

The input.txt file should contain one user ID per line.
