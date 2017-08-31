from kafka import KafkaProducer

import couchdb
import json
import tweepy

class Listener(tweepy.StreamListener):
    def __init__(self):
        #self.producer = KafkaProducer(bootstrap_servers = '127.0.0.1:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
	self.couch = couchdb.Server()
	self.db = self.couch['tweets']

    def on_data(self, data):
        try:
            msg = json.loads( data )
            screen_name = msg['user']['screen_name'].encode('utf-8')
            tweet_text = msg['text'].encode('utf-8')
            print screen_name + ' tweets:'
            print tweet_text
            print '------------------'
            print

            self.db.save(msg)

            return True
        except BaseException as e:
            print 'Error on_data: ' + str(e)
            print '------------------'
            print
            return True

    def on_error(self, status_code):
        if status_code == 420:
            print 'Possible Rate limiting error.'
            return False
        else:
            return True

def main():
    consumer_key = '9XUx4HdhMURjELBO9PT6eWCIA'
    consumer_secret = 'utmsvuQUSdBUkM5axLtIjVtr1CHGMPl5QDxypVUrk7d713LIKd'

    access_token = '400677164-UIeik6D9nWFsVYDjUkcUECClraWmVnDDpY2vw5hj'
    access_token_secret = 'bl8CTeUHH0tRTpUR5kto5WYf8wDVSKNdQSSa9knc9gOnv'

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = tweepy.Stream(auth, Listener())
    stream.filter(locations = [116.930, 4.650, 126.600, 20.840])

if(__name__ == '__main__'):
    main()
