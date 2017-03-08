#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import psycopg2
import json
import os
import pandas as pd

#Variables that contains the user credentials to access Twitter API 
access_token = "2777671802-F34NQ9bKSYXSfGM05wXNF4hgvGmbybpunmgIpRO"
access_token_secret = "8gZkglWGss4h6nkfOGNUaQ8gobCQsPrWC0ryx9f4Fatbz"
consumer_key = "7LXhhD8ilnEQrONGGOLxpA3eN"
consumer_secret = "QX2axfCrQdoj5P5t0YmZ7z9zHJ0Z4qc4uMpa2tY5qfugsejLNK"


#This is a basic listener that just prints received tweets to stdout.
twitterCount = 0
columns = ['id', 'location', 'timestamp', 'text']
twitterRecord = pd.DataFrame(columns = columns)
class StdOutListener(StreamListener):
    
    
    def on_data(self, data):
    	try:
	    	global twitterRecord
	    	global twitterCount
	    	twitterCount += 1
	    	data = json.loads(data)
	    	idInfo = data['id']
	    	locationInfo = data['geo']['coordinates'] if data['geo'] is not None else[0, 0]
	    	timestampInfo = int(data['timestamp_ms'])
	    	textInfo = data['text']
	    	twitterRecord.loc[len(twitterRecord)] = [idInfo, locationInfo, timestampInfo, textInfo]
	    	print twitterCount
	    	if len(twitterRecord) == 1000:
	    		self.insert_data()
    		# twitterRecord = twitterRecord[0:0]
    	except KeyError:
    		print "Key Error"
    	except ValueError:
    		print "Value Error"
    	return True

    def insert_data(self):
    	global twitterRecord
        conn_string = "host='localhost' dbname='twitterdata' user='mingchen' password='123456'"
        print "Connecting to database\n ->%s" % (conn_string)
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        # query = "INSERT INTO twitterinfo(id, location, timestamp, text ) VALUES (%d, [%d, %d], %d, %s);"
        queryInsertTwitterinfo = "INSERT INTO twitterinfo VALUES (%s, %s, %s);"
        queryInsertLocation = "INSERT INTO location VALUES (%s, %s, %s);"

        try:
            for i, row in twitterRecord.iterrows():
            	dataTwitterInfo = (row['id'], row['timestamp'], row['text'],)
            	dataLocation = (row['id'], row['location'][0], row['location'][1],)
            	cursor.execute(queryInsertTwitterinfo, dataTwitterInfo)
            	cursor.execute(queryInsertLocation, dataLocation)

        except KeyError:
            print "Key Error"
        except ValueError:
            print "value Error"

        conn.commit()
        twitterRecord = twitterRecord[0:0]
        conn.commit()
        print "database update done!    "


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