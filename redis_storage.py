import redis
import Query
import logging

class RedisStorage(object):
	"""docstring for Redistorage"""

	default_ttl = 3600*24*3

	def __init__(self, host_, port_, db_):
		self.connect = redis.StrictRedis(host=host_, port=port_, db=db_)
		#print("created")

	def put_chunk(self, chunk, ttl = None):
		
		dict_chunk = {	"t1" : chunk.t1,
						"t2" : chunk.t2, 
						"channels" : ",".join(list(map(str,chunk.ch))), 
						"next" : chunk.next.uuid if chunk.next else None,
						"prev" : chunk.prev.uuid if chunk.prev else None}
		

		self.connect.hmset("{}".format(chunk.uuid), dict_chunk)
		if ttl is None:
			ttl = self.default_ttl
		self.connect.expire(chunk.uuid, ttl)

	def get_chunk(self, uuid):
		chunk = self.connect.hgetall("{}".format(uuid))
		if chunk is None:
			logging.error("error")
		ch = chunk[b"channels"].decode('utf-8')		
		array = []	
		s = ch[:ch.find(",")]
		print(s)
		if(s.isdigit()):
			array = list(map(int,ch.split(",")))
		else:
			array = list(map(str,ch.split(",")))
		print(array)
		return {"t1" : float(chunk[b"t1"]), "t2" : float((chunk[b"t2"])), "channels" : array, "next" : chunk[b"next"], "prev" : chunk[b"prev"]}


		
