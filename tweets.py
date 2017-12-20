import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
from kafka import SimpleProducer, KafkaClient

class MyListener(StreamListener):
 
    def on_data(self, data):
        try:
            producer.send_messages("twitter", data.encode('utf-8'))
            print(data, type(data))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True
 
    def on_error(self, status):
        print(status)
        return True    

consumer_key = 'UZ4u0kuF5i4uRXcxSk5Iv8ZnJ'
consumer_secret = 'wLMtGwt8WxMWgqpipU28xOYPTrTW72znEQsCgS801XTXyYZIqU'
access_token = '183456396-hzR4O5iUBVw6lZMsUPzbFi6wg6fYgf77Mbt7ZZvr'
access_secret = 'nXLmRoQ0XCM4j3e7tKrQzQyt88UpKxWrrTDOxO4iyEh5t'
kafka = KafkaClient("172.16.26.105:9092")
producer = SimpleProducer(kafka)

 
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
 
api = tweepy.API(auth)

twitter_stream = Stream(auth, MyListener())

def streamTweets(query):
    if twitter_stream.running is True:
        print("stream running")
        twitter_stream.disconnect()
        print("stream disconnected")
    twitter_stream.filter(track=query.split(","))
    return
