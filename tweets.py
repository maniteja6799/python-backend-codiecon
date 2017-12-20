import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
from kafka import SimpleProducer, KafkaClient

from threading import Thread
import logging
from elasticsearch import Elasticsearch

es = Elasticsearch([{"host": "localhost", "port": 9200}])
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)
ELASTICSEARCH_INDEX = "twitter"
ELASTICSEARCH_TYPE = "tweet"
INDEX_BODY_PATH = "resources/index_settings_mappings.json"


consumer_key = 'UZ4u0kuF5i4uRXcxSk5Iv8ZnJ'
consumer_secret = 'wLMtGwt8WxMWgqpipU28xOYPTrTW72znEQsCgS801XTXyYZIqU'
access_token = '183456396-hzR4O5iUBVw6lZMsUPzbFi6wg6fYgf77Mbt7ZZvr'
access_secret = 'nXLmRoQ0XCM4j3e7tKrQzQyt88UpKxWrrTDOxO4iyEh5t'
kafka = KafkaClient("172.16.26.105:9092")
producer = SimpleProducer(kafka)

flag = False
query = ""
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
	
api = tweepy.API(auth)
twitter_stream = Stream(auth, MyListener())

def pollCurrentStreaming():
	global flag
	while True:
		if flag is True:
			if threadStream.isAlive() is False:
				threadStream.start()
			else:
				if twitter_stream.running is True:
					twitter_stream.disconnect
				threadStream.stop()
				threadStream.start()
				flag = False

def processStream(input_query):
	global query, flag 
	query = input_query
	flag = True
	if threadPoll.isAlive() is False:
		threadPoll.start();


def startStreaming():
	global twitter_stream
	if twitter_stream.running is True:
		twitter_stream.disconnect
		twitter_stream.filter(track=query.split(","))


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

threadPoll = Thread(target= pollCurrentStreaming)
threadStream = Thread(target = startStreaming)

def recreateIndex():
    LOG.info("recreating index : " + ELASTICSEARCH_INDEX + " with file: " + INDEX_BODY_PATH)
    es.indices.delete(index= ELASTICSEARCH_INDEX)
    with open(INDEX_BODY_PATH, "r") as f:
        index_body = f.read()
        es.indices.create(index= ELASTICSEARCH_INDEX, body= index_body)
