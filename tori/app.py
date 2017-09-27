import couchdb, itertools, json, tweepy


class Listener(tweepy.StreamListener):

    def __init__(self, db):
        self.db = db
        self.n = 0
        self.most_recent = 0
        self.places = []

    def on_data(self, data):
        try:
            msg = json.loads( data )

            timestamp_ms = int(msg['timestamp_ms'])
            hash_tags = map(lambda f: f['text'], msg['entities']['hashtags'])
            screen_name = msg['user']['screen_name'].encode('utf-8')
            place_full_name = msg['place']['full_name'].encode('utf-8')

            self.places_add(place_full_name, timestamp_ms)

            self.n = self.n + 1
            if self.n % 10 == 0:
                self.places_window(3600000)

                for kv in self.places_top_k(10):
                    print '%s %d' % (kv[0], kv[1])

                print '------------------'
                print

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


    def places_add(self, place, timestamp_ms):
        # update most recent
        if self.most_recent < timestamp_ms:
            self.most_recent = timestamp_ms

        self.places.append((place, timestamp_ms))

    def places_top_k(self, k):
        grouped = itertools.groupby(map(lambda kv: (kv[0], 1), self.places), lambda kv: kv[0])
        for_reduce = map(lambda kv : (kv[0], map(lambda n: n[1], kv[1])), grouped)
        reduced = map(lambda kv: (kv[0], sum(kv[1])), for_reduce)
        return sorted(reduced, key = lambda kv: kv[1], reverse = True)[:k]

    def places_window(self, window):
        earliest = self.most_recent - window
        self.places = filter(lambda t: t[1] > earliest, self.places)





def main():


    consumer_key = '9XUx4HdhMURjELBO9PT6eWCIA'
    consumer_secret = 'utmsvuQUSdBUkM5axLtIjVtr1CHGMPl5QDxypVUrk7d713LIKd'

    access_token = '400677164-UIeik6D9nWFsVYDjUkcUECClraWmVnDDpY2vw5hj'
    access_token_secret = 'bl8CTeUHH0tRTpUR5kto5WYf8wDVSKNdQSSa9knc9gOnv'

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    #couch = couchdb.Server()
    #db = couch['tweets']

    listener = Listener(None)

    stream = tweepy.Stream(auth, listener)
    stream.filter(locations = [116.930, 4.650, 126.600, 20.840])

if(__name__ == '__main__'):
    main()
