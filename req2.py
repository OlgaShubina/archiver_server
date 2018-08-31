import simplejson as json
from collections import defaultdict
import http.client
import logging 
import time
import matplotlib.pyplot as plt
import datetime
import csv

class Req(object):
	"""docstring for """
	def __init__(self, url, host, localhost):
		self.url = url
		self.host = host
		self.localhost = localhost

	def post_freq(self, data, uuid):
		data_js = json.dumps(data).encode('utf-8')
		headers = {'Content-type': 'application/json'}
		conn = http.client.HTTPConnection(self.host, self.localhost)
		conn.request("POST", "/chunk/"+str(uuid), data_js, headers)
		response = conn.getresponse()
		return response

	def get_freq(self, response):			
		js = json.load(response)
		return js

	def get_chunk(self, uuid):
		headers = {'Content-type': 'application/json'}
		conn = http.client.HTTPConnection(self.host, self.localhost)
		conn.request("GET", "/chunk/"+str(uuid), {}, headers)		
		response = conn.getresponse()
		return response	

	def get_chunk_compress(self, uuid, fields, level):
		headers = {'Content-type': 'application/json'}
		data = {"fields" : fields, "level" : level}
		data_js = json.dumps(data).encode('utf-8')
		conn = http.client.HTTPConnection(self.host, self.localhost)
		conn.request("POST", "/chunk_compress/"+str(uuid), data_js, headers)		
		response = conn.getresponse()
		return response	

	def Cursor(self, t1, t2, channels, compress_level, process_method=None):
		try:
			data = {"t1" : t1, "t2" : t2, "channels" :  channels}
			data_js = json.dumps(data).encode('utf-8')
			headers = {'Content-type': 'application/json'}
			conn = http.client.HTTPConnection(self.host, self.localhost)
			conn.request("POST", "/cursor", data_js, headers)
			response = conn.getresponse()
			js = json.loads(response.read())
			data = response.read()
			count = 0
			current_uuid = js["first"]["uuid"]
			length = js["len_dict"]
			print(js)
			while(count<length):
				r =self.get_chunk(current_uuid)
				current_chunk = self.get_freq(r)
				yield current_chunk
				current_uuid = current_chunk.get("next", None)				
				if current_uuid is None:
					break				
				count+=1
			return js
		except KeyError:
			logging.error("1")

	def CursorCompress(self, t1, t2, channels, level, fields=None):
		try:
			data = {"t1" : t1, "t2" : t2, "channels" :  channels, "fields" : fields, "level" : level}

			data_js = json.dumps(data).encode('utf-8')
			headers = {'Content-type': 'application/json'}
			conn = http.client.HTTPConnection(self.host, self.localhost)
			conn.request("POST", "/cursor_compress", data_js, headers)
			response = conn.getresponse()
			js = json.loads(response.read())
			data = response.read()
			count = 0
			current_uuid = js["first"]["uuid"]
			length = js["len_dict"]
			while(count<length):
				r =self.get_chunk_compress(current_uuid, fields, level)
				current_chunk = self.get_freq(r)
				yield current_chunk
				current_uuid = current_chunk.get("next", None)
				if current_uuid is None:
					break				
				count+=1
			return js
		except KeyError:
			logging.error("1")


if __name__ =='__main__':
	f = open("result","w")
	req = Req("http://127.0.0.1:5000/", "127.0.0.1", 5000)
	array1 = []
	array2 = []
	array3 = []
	array4 = []
	x1 = []
	x2 = []
	x3 = []
	x4 = []
	y1 = []
	y2 = []
	y3 = []
	y4 = []

	t1 = "2017-05-25 23:00:00.00000"
	t2=  "2017-05-26 04:00:00.00000"
	channels = ["VEPP/BPM/1/x"]
	channels = ["VEPP/Energy/Energy_NMR"]
	channels = ["SND/EMS/DE", "VEPP/Energy/Energy_NMR", "VEPP/BPM/1/x"]

	for i in req.CursorCompress(t1, t2, channels, "raw"):
		try:			
			array1.append(i)
			
		except(KeyError):
			pass
	for ch in channels:	
		try:	
			with open(str(ch).replace('/', '_')+'.csv', 'w') as csvfile:
				wrtr = csv.DictWriter(csvfile, fieldnames=['time', 'value'])
				for i in array1[0][ch]:
					wrtr.writerow({"time":str(datetime.datetime.strptime(i["time"],'%Y-%m-%d %H:%M:%S.%f')),"value": str(i["value"])})
		except(KeyError):
			pass