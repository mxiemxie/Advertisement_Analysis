#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#Variables that contains the user credentials to access Twitter API 
access_token = "2777671802-F34NQ9bKSYXSfGM05wXNF4hgvGmbybpunmgIpRO"
access_token_secret = "8gZkglWGss4h6nkfOGNUaQ8gobCQsPrWC0ryx9f4Fatbz"
consumer_key = "7LXhhD8ilnEQrONGGOLxpA3eN"
consumer_secret = "QX2axfCrQdoj5P5t0YmZ7z9zHJ0Z4qc4uMpa2tY5qfugsejLNK"


#This is a basic listener that just prints received tweets to stdout.
twitterCount = 0
class StdOutListener(StreamListener):
    
    
    def on_data(self, data):
        with open('TwitterData.json', 'a') as f:

            f.write(data)
            global twitterCount
            twitterCount += 1
            print (twitterCount)
            return True
        # except BaseExeption as e:
        #     print("BaseExeption")

    def on_error(self, status):
        print status
        return True


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    stream.filter(track=['Nike', 'Adidas', 'AJ', 'UA', 'PUMA'])