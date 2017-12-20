import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
from kafka import SimpleProducer, KafkaClient

from threading import Thread
import logging
from elasticsearch import Elasticsearch

es = Elasticsearch([{"host": "172.16.28.35", "port": 9200}])
LOG = logging.getLogger("tweets")
LOG.setLevel(logging.DEBUG)
ELASTICSEARCH_INDEX = "twitter"
ELASTICSEARCH_TYPE = "tweet"
INDEX_BODY_PATH = "resources/index_settings_mappings.json"

flag = False
query = ""

class MyListener(StreamListener):

	def on_data(self, data):
		try:
			producer.send_messages("twitter", data.encode('utf-8'))
			print(data)
			return True
		except BaseException as e:
			print("Error on_data: %s" % str(e))
		return True

	def on_error(self, status):
		print(status)
		return True    

class ThreadStreaming(Thread):
	flagStartStreaming = False;
	def __init__(self):
		super(ThreadStreaming, self).__init__()
	def startStreaming(self):
		global twitter_stream, flag
		self.flagStartStreaming = False
		twitter_stream.filter(track=query.split(","))
	def run (self):
		while True:
			if self.flagStartStreaming is True:
				self.startStreaming()
	def setFlagStartStreaming(self, var):
		self.flagStartStreaming = var	

threadStream = ThreadStreaming()	

class ThreadPoll(Thread):
	def __init__(self):
		super(ThreadPoll, self).__init__()
	def stopStreaming(self):
		global twitter_stream
		if twitter_stream.running is True:
			twitter_stream.disconnect() 		
	def run(self):
		global threadStream, flag, recreateIndex
		while True:
			if flag is True:
				self.stopStreaming()
				recreateIndex()
				flag = False
				threadStream.setFlagStartStreaming(True)
threadPoll = ThreadPoll()

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
print("Tweets: everything initialised")
def processStream(input_query):
	global query, flag , threadPoll
	print("processing stream: " + input_query)
	query = input_query
	flag = True
	if threadPoll.is_alive() is False:
		print("starting threadpoll")
		threadPoll.start()
	if threadStream.is_alive() is False:
		print("starting threadStream")
		threadStream.start()

def recreateIndex():
    print("recreating index : " + ELASTICSEARCH_INDEX + " with file: " + INDEX_BODY_PATH)
    es.indices.delete(index= ELASTICSEARCH_INDEX)
    with open(INDEX_BODY_PATH, "r") as f:
        index_body = f.read()
        es.indices.create(index= ELASTICSEARCH_INDEX, body= index_body)
