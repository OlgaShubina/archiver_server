import simplejson as json
from collections import namedtuple, defaultdict
import datetime
import pytz
import urllib.request
#import Archive
import redis_storage
import uuid
import logging
from parsconfig import Config

config = Config('config.yaml')


ttl = 1000

class ChunkId(object):
	"""docstring for UUID"""
	def __init__(self):
		self.chunk_id = 20

	def get_uuid(self):
		self.chunk_id = str(uuid.uuid1().hex)
		return self.chunk_id

chunk_uuid = ChunkId()

class Chunk:
	"""docstring for Chunk"""
	def __init__(self, t1, t2, ch):
		self.t1 = t1
		self.t2 = t2
		self.ch = ch
		self.uuid  = chunk_uuid.get_uuid()
		self.next = None
		self.prev = None

	def get_chunk(self, chunk, data_base):
		data = data_base.get_data(chunk.t1, chunk.t2, chunk.ch)
		if data is None:
			logging.error("chunk not founded")
		d = defaultdict(list)
		for i in data:
			d[i[1]].append({"time" : i[0].timestamp(), "value" : i[2]})
		js = json.dumps(d)
		return js

	def next_chunk(self, chunk):
		self.next = chunk

	def prev_chunk(self, chunk):
		self.prev = chunk

	def to_json(self):
		chunk = {"t1" : self.t1, "t2" : self.t2, "channels" : self.ch,
				 "channels_id" : self.uuid,
				  "next" : self.next.uuid if self.next else None,
				  "prev" : self.prev.uuid if self.prev else None}
		js = json.dumps(chunk)
		return js 

	def __str__(self):
		return self.to_json()

class ChunkInfoStorage(object):
	"""docstring for ChunkInfoStorage"""
	def __init__(self, ttl):
		self.ttl = ttl
		self.dict_chunk = {}	

	def put(self, chunk):
		self.dict_chunk.update({chunk.uuid : {"chunk" : chunk, "create_time" : datetime.datetime.now().timestamp()}})
		return chunk.uuid

	def get(self, uuid):

		if((self.dict_chunk[uuid]["create_time"]+ttl) > datetime.datetime.now().timestamp()):
			return self.dict_chunk[uuid]
		else:
			self.remove(uuid)
			logging.error("error")
			return None
		
	def remove(self, uuid):
		del self.dict_chunk[uuid]
		
class Query(object):
	@classmethod
	def setup_storage(cls, storage):
		cls.storage = storage

	"""docstring for Query"""
	def __init__(self, config, t1, t2, delta=1):
		try:
			self.size_stream = config["size_stream"]
			self.density = config["density"]
			self.t1 = (datetime.datetime.strptime(t1,'%Y-%m-%d %H:%M:%S.%f')).timestamp()
			self.t2 = (datetime.datetime.strptime(t2,'%Y-%m-%d %H:%M:%S.%f')).timestamp()
			self.delta = delta

		except ValueError:
			logging.error("data is not correct")
				
	def separate(self, channels):
		try:
			storage = redis_storage.RedisStorage(config.storage["host"], config.storage["port"], config.storage["chunk_db"])
			count = (self.t2-self.t1)*self.density*len(channels)//(self.size_stream*self.delta)
			print(count)
			if(count==0):
				count=1
			deltaT = (self.t2-self.t1)//count
			array = []
			t = self.t1
			while t < self.t2:
				chunk = Chunk(t, t+deltaT, channels)			
				array.append(chunk)
				t += deltaT
			if(t!=self.t2):	
				chunk = Chunk(t+deltaT, self.t2, channels)			
				array.append(chunk)

			for i,c in enumerate(array[1:]):
				array[i].next = c
				c.prev = array[i]

			for i in array:
				storage.put_chunk(i)
			return array
		except AttributeError:
			raise
			logging.error("atribute error")
	

