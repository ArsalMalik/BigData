from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from elasticsearch import Elasticsearch
from datetime import datetime

# Assigning host and port name
TCP_IP = 'localhost'
TCP_PORT = 9001
# Index Name in which data will be inserted in elasticsearch
INDEX_NAME='analysis_twitter'

# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[4]')

# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("OFF")

# create the Streaming Context from spark context with interval size 2 seconds
ssc = StreamingContext(sc, 2)
ssc.checkpoint("checkpoint_TwitterApp")

# Evaluate the sentiment of the text of the tweet
# using nltk
def evaluate(text):
    sentiment = SentimentIntensityAnalyzer()
    polarity = sentiment.polarity_scores(text)

    if(polarity["compound"] > 0):
        return "positive"
    elif(polarity["compound"] < 0):
        return "negative"
    else:
        return "neutral"

# Send the tweet and meta data to elasticsearch
def sendToEs(line):
    # initialize elasticsearch with read timeout = 1200
    es = Elasticsearch([{'host':'localhost', 'port':9200}], http_auth=('elastic', 'elastic'), timeout=3600)
    
    # get values from rdd
    tweet = line[3]
    sentiment = line[0]
    latitude = line[1]
    longitude = line[2]
    
    #create elasticsearch index
    doc = {
        "tweet": tweet,
        "sentiment": sentiment,
        "location":[longitude, latitude], #Type defined as geo_point in mapping
        "timestamp": datetime.now()
    }
    
    # Insert data from rdd into elasticsearch 
    res = es.index(index=INDEX_NAME, doc_type="_doc",body=doc)
    # Print response to the console
    print "Response: {}".format(res)


# read data from localhost on port 9001
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)
# Print first ten elements of each rdd in the Dstream to console
dataStream.pprint()

# Splitting the rdds by tab and applying map transformation
sentiment_loc = dataStream.map(lambda line:line.split("\t"))\
                .map(lambda line: (evaluate(line[0]), float(line[1]), float(line[2]), line[3]))

# sending data from individual rdds to elasticsearch
sentiment_loc.foreachRDD(lambda rdd : rdd.foreach(sendToEs))

# Start computation
ssc.start()
#wait for the computation to terminate
ssc.awaitTermination()
