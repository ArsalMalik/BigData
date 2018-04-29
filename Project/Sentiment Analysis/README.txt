=======================
        README 
=======================

Versions Used,

Python        - 2.7.12
Spark         - 2.3.0
Elasticsearch - 6.2.3
Kibana        - 6.2.3

To get things started,
Ran elasticsearch and Kibana and open them in browser.

To create a new index in elasticsearch with mapping for a geo_point
wrote the following command in kibana console,

PUT analysis_twitter
{
    "mapping": {
        "_doc": {
            "properties": {
                "location": {
                    "type": "geo_point"
                }
            }
        }
    }
}

Then ran the spark-job for the scrapper file to get tweets from twitter using tweepy
and then submitted spark job for spark streaming file.

It will receive the tweets and location meta data via socket, 
and will evaluate the sentiment of the tweet using Natural Language Toolkit (nltk).

Then created a mapping with the same name as the index created above,
and sent the tweets, the sentiment and their locations to elasticsearch.

Created visualizations on the Kibana dashboard such as ,
Data Table -  Tweets | Sentiment 
Co-ordinate Maps,
- All tweets and their location
- Negative tweets and their locations
- Positive tweets and their locations

=======================
     How to Run
=======================
1) Run elasticsearch,
   elasticsearch-6.2.3/bin/elasticsearch
2) Run Kibana,
   kibana-6.2.3/bin/kibana
3) Run the spark-job for the scrapper file,
   spark-submit stream.py
4) Run the spark job for the Spark streaming file,
   spark-submit spark.py
  