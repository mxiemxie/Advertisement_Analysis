#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import psycopg2
import json
import os

#Variables that contains the user credentials to access Twitter API 
access_token = "2777671802-F34NQ9bKSYXSfGM05wXNF4hgvGmbybpunmgIpRO"
access_token_secret = "8gZkglWGss4h6nkfOGNUaQ8gobCQsPrWC0ryx9f4Fatbz"
consumer_key = "7LXhhD8ilnEQrONGGOLxpA3eN"
consumer_secret = "QX2axfCrQdoj5P5t0YmZ7z9zHJ0Z4qc4uMpa2tY5qfugsejLNK"


#This is a basic listener that just prints received tweets to stdout.
twitterCount = 0
class StdOutListener(StreamListener):
    
    
    def on_data(self, data):

        with open('twitterData.json', 'a') as f:
            f.write(data)
            global twitterCount
            twitterCount += 1
            if twitterCount % 10 == 0:
                self.insert_data('twitterData.json')
            print twitterCount
            return True

    def insert_data(self, fileName):
        conn_string = "host='localhost' dbname='twitterdata' user='mingchen' password='123456'"
        print "Connecting to database\n ->%s" % (conn_string)
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        with open(fileName, 'rb') as f:
                        oriData = f.readlines()
                        for i in range(len(oriData)):
                            try:
                                rowData = json.loads(oriData[i])
                                text = rowData["text"]
                                cursor.execute("INSERT INTO twittercontent VALUES (%s);",(text,))
                            except KeyError:
                                print "Key Error"
                            except ValueError:
                                print "value Error"
                        conn.commit()
        print "database update done!    " + fileName
        os.remove(fileName)


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