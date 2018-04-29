import json, tweepy, socket, re 
from nltk.tokenize import TweetTokenizer
from nltk.corpus import stopwords
import nltk
from geolocation.main import GoogleMaps

# Authorization tokens to use tweepy API
ACCESS_TOKEN = '213398955-nuIELW0EBS0uKNoa4pz5nsA5PK7z05SnaNLaXOzY'
ACCESS_SECRET = 'X9UpKrFFTGaZqrmH2ln4eBlyHOssdEBdrFD3mKZz6lwgZ'
CONSUMER_KEY = 'pqS0lmjWDbtLLE5iztHA4xSh5'
CONSUMER_SECRET = 'EphloTtqg5uuY7oDcm2QxTc2GFzRKTXIcvOhapiR9Gf77cC1EA'

# Authorizing tweepy 
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

#Hashtag to track in the tweets
hashtag = '#guncontrolnow'

# Host and port names
TCP_IP = 'localhost'
TCP_PORT = 9001


# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()


# pre-processing the tweet
def preprocess(text):
    tokenizer = TweetTokenizer()
    stop_words_english = stopwords.words('english')
    emoticons_str = r"""
                        (?:
                        [<>]?
                        [:;=8]                     # eyes
                        [\-o\*\']?                 # optional nose
                        [\)\]\(\[dDpP/\:\}\{@\|\\] # mouth
                        |
                        [\)\]\(\[dDpP/\:\}\{@\|\\] # mouth
                        [\-o\*\']?                 # optional nose
                        [:;=8]                     # eyes
                        [<>]?
                        |
                        <3                         # heart
                        )"""
    mentions_str = r'(?:@[\w_]+)' # @mentions
    url_str = r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+' # URLs

    # remove symbols excluding the \s symbol
    text = re.sub(emoticons_str, '', text)
    text = re.sub(mentions_str, '', text)
    text = re.sub(url_str, '', text)
    # tokenize the tweet
    text = tokenizer.tokenize(text)
    # Remove any words with length smaller than 3 and remove numbers and stop words
    words = [w.lower() for w in text if len(w)>2 and not w.isdigit() and not w.lower() in stop_words_english]
    # Create back the string from tokens
    words_string = " ".join(words)
    # Remove tweets not belonging to the hashtag
    if hashtag in words_string:
        return words_string
    else:
       return 1


class MyStreamListener(tweepy.StreamListener):
    def on_status(self, status):
       status = status.text.encode('utf-8')
       print(status)
       conn.send(status)
    
    def on_data(self, data):
        #Decode json data
        tweet = json.loads(data)
        username = tweet['user']['screen_name']
        location_address = tweet['user']['location']
        tweet_text = tweet['text'].encode('ascii', 'ignore')
        
        processed_tweet = preprocess(tweet_text)
        if processed_tweet != 1:
            try:
                if location_address and location_address is not None:
                    location_address.replace(" ", "+")
                    google_maps = GoogleMaps(api_key='AIzaSyCoMZR7wii10qjJarohY7Ru__9cxC8Ftgw')
                    geo_location = google_maps.search(location=location_address)
                    location = geo_location.first()
                
                    if location and location is not None:
                        latitude = location.lat
                        longitude = location.lng 
                        
                        print("Original token list:", tweet_text)
                        print("Location: ", location_address)
                        print("New token list:", processed_tweet)
                        print "Latitude: {}, Longitude= {} ".format(latitude, longitude) 
                        #print("Sentiment: ", sentiment)
                        tweet = " ".join(tweet_text.split())
                        processed_tweet = (processed_tweet+"\t"+str(latitude)+"\t"+str(longitude)+"\t"+tweet+"\n")
                        conn.send(processed_tweet.encode('utf-8'))
                    else:
                        print("No location returned from the API!")    
                else:
                    print("No location data associated with the tweet!")
            except:  
                print("Couldn't find location of the tweet!" )
        
            
    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)

myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())

myStream.filter(track=[hashtag])


