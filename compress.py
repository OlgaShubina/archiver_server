from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
import sqlalchemy
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from collections import namedtuple, defaultdict
from sqlalchemy.orm import mapper
import datetime
import math
import simplejson as json
from table import Table, Archive, LogDouble, LogText
from channels import Channels_
import numpy as np
#import matplotlib.pyplot as plt
from process_table import CompressDouble,  DSDouble, DSText,  DSDoubleSecond,  DSDoubleThird,  CompressDoubleSecond, CompressDoubleThird, DSTextSecond, DSTextThird
import time
from parsconfig import Config

config = Config('config.yaml')
Base = declarative_base()
url = 'postgresql://{}:{}@{}:{}/{}'.format(config.archive["database"]["user"], config.archive["database"]["password"], config.archive["database"]["host"], config.archive["database"]["port"], config.archive["database"]["dbname"])
engine = sqlalchemy.create_engine(url, client_encoding='utf8') 
Session = sessionmaker(bind=engine)

import logging

url_compress = 'postgresql://{}:{}@{}:{}/{}'.format(config.compress_archive["database"]["user"], config.compress_archive["database"]["password"], config.compress_archive["database"]["host"], config.compress_archive["database"]["port"], config.compress_archive["database"]["dbname"])
engine_compress = sqlalchemy.create_engine(url_compress, client_encoding='utf8') 
Session_compress = sessionmaker(bind=engine_compress)
sesion_compress = Session_compress()

class Compress(object):
	"""docstring for Compress"""
	def __init__(self, t1, delta, dict, table, compress_level):
		self.table = table
		self.insert_table = Table(table.name, sesion_compress)
		self.t1 = t1
		self.delta = delta
		self.table = table
		self.dict = dict
		self._mapping = {'numeric': LogDouble, 'text' : LogText}
		self.dict_table ={"first_level" : {"average_double" : CompressDouble, "ds_double" : DSDouble, "ds_text" : DSText, "name_table" : ["average", "ds_double", "ds_text"]}, 
							"second_level" : {"average_double" : CompressDoubleSecond, "ds_double" : DSDoubleSecond, "ds_text" : DSTextSecond, "name_table" : ["average_second", "ds_double_second", "ds_text_second"]},
							"third_level" : {"average_double" : CompressDoubleThird, "ds_double" : DSDoubleThird, "ds_text" : DSTextThird, "name_table" : ["average_third", "ds_double_third", "ds_text_third"]} }
		self.compress_level = compress_level

	def sum_data(self, data):
		return sum(data)

	def median_data(self, value, lenght):
		value.sort()
		return  np.median(value)
		if lenght%2==0 and lenght!=2 and lenght!=0:
			mediana = (value[lenght//2]+value[lenght//2+1])/2
		elif lenght ==1:
			mediana = value[0]
		elif lenght == 2:
			mediana = (value[0]+value[1])/2
		elif lenght ==0:
			mediana = 0
		else:
			mediana = value[lenght//2+1]

		return mediana

	def sygma(self, value, lenght, average):
		summa = 0
		for i in value:
			try:
				summa += (i - average)**2
			except:
				print(i,summa, average)
				raise
		sygma = math.sqrt(summa/lenght)
		return sygma

	def most_freq(self, value):
		d = defaultdict(list)
		array = []
		for i in value:
			d[value.count(i)] = i  
		for count, element in d.items():
			array.append(count)
		return d[max(array)]
	def center(self, value):
		lenght = len(value)
		center = value[lenght//2]
		return center

class Average(Compress):
	"""docstring for Average"Compress"""
	def __init__(self, t1, delta, dict, table, compress_level):
		super().__init__(t1, delta, dict, table, compress_level) 
		if self.table.name == self._mapping["numeric"]:
			self.compress_table = self.dict_table[compress_level]["average_double"]
			self.average = self.calc_average()

	def calc_average(self):
		for ch, i in self.dict.items():
			lenght = len(i)
			value = (m.value for m in i)
			if lenght == 0:
				avg = None
				mediana = None
				maximum = None
				minimum = None
				syg = None					
			else:
				list_ = value
				l = list(filter(lambda x: not math.isnan(x), list_))
					
				lenght = len(l)
				if lenght == 0:
					mediana = None
					maximum = None
					minimum = None
				else:
					mediana = self.median_data(l, lenght)
					minimum = l[0]
					maximum = l[-1]
				ll = list(filter(lambda x: not math.isinf(x), l))
				lenght = len(ll)
				if lenght == 0:
					syg = None	
					avg = None
				else:
					avg = self.sum_data(ll)/lenght					
					syg = self.sygma(ll, lenght, avg)
			self.insert_table.insert_table(self.compress_table(datetime.datetime.fromtimestamp(self.t1),self.delta, ch, avg, mediana, minimum, maximum, syg))
		

class DataSampler(Compress):
	"""docstring for Statictic"Compress"""
	def __init__(self, t1, delta, dict, table, compress_level):
		super().__init__(t1, delta, dict, table, compress_level)
		if table.name == self._mapping["numeric"]:
			self.compress_table = self.dict_table[compress_level]["ds_double"]
		elif table.name == self._mapping["text"]:
			self.compress_table = self.dict_table[compress_level]["ds_text"]
		self.average = self.calc_static()


	def calc_static(self):
		for ch, i in self.dict.items():
			value = list(m.value for m in i)
			left_bound = value[0]
			right_bound = value[-1]
			most_freq = self.most_freq(value)
			center = self.center(value)
#			print(self.compress_table, left_bound, right_bound, most_freq, center)
			self.insert_table.insert_table(self.compress_table(datetime.datetime.fromtimestamp(self.t1), self.delta, ch, left_bound, right_bound, most_freq, center))

	
class Process(object):
	"""docstring for Process"""
	def __init__(self, session):
		self._tables = {}
		self.session = session()
		self.dict_fields = {"average" : ("min", "max", "avg", "mediana", "sygma"), "data_sampler" : ("left_bound", "right_bound", "center", "most_freq")}
		self.dict_table ={"first_level" : {"numeric" : {"average" : CompressDouble, "data_sampler" : DSDouble}, "text" : {"data_sampler" : DSText}}, 
							"second_level" : {"numeric" : {"average" : CompressDoubleSecond, "data_sampler" : DSDoubleSecond}, "text" : {"data_sampler" : DSTextSecond}},
							"third_level" : {"numeric" :{"average" : CompressDoubleThird, "data_sampler" : DSDoubleThird}, "text" : {"data_sampler" : DSTextThird}}}
		self._mapping = {'numeric': LogDouble, 'text' : LogText}
		for dtype, table_name in self._mapping.items():
			self._tables[dtype] = Table(table_name, self.session)

	def processed_data(self, t1, t2, dict_interval, channels=None):
		
		ch = Channels_()
		table_channels = ch.get_channels(self.session)
		dict_channels  = ch.found_type(table_channels, channels)
		for t, chans in dict_channels.items():
			if t!="event":			
				for name, delta in dict_interval.items():
					t1_ = datetime.datetime.strptime(t1,'%Y-%m-%d %H:%M:%S.%f').timestamp()
					t2_ = datetime.datetime.strptime(t2,'%Y-%m-%d %H:%M:%S.%f').timestamp()
					while t1_ < t2_:			
						data = self._tables[t].select_table(datetime.datetime.fromtimestamp(t1_), datetime.datetime.fromtimestamp(t1_+ delta), chans)
#						print(delta)
						d = defaultdict(list)
						for i in data:
							d[i.ch_id].append(i)	
						compress1 = Average(t1_, delta, d, self._tables[t], name)
						compress2 = DataSampler(t1_, delta, d, self._tables[t], name)
						t1_+=delta	
						sesion_compress.commit()
		return True

	def processed_data2(self, t1, t2, dict_interval, channels=None):
		
		ch = Channels_()
		table_channels = ch.get_channels(self.session)
		dict_channels  = ch.found_type(table_channels, channels)
		for t, chans in dict_channels.items():
			if t!="event":		
				l = lambda x: x[1]
				new_dict =  sorted(dict_interval.items(), key=l, reverse=True) 
#				print(new_dict)
				for delta in new_dict:
					t1_ = datetime.datetime.strptime(t1,'%Y-%m-%d %H:%M:%S.%f').timestamp()
					t2_ = datetime.datetime.strptime(t2,'%Y-%m-%d %H:%M:%S.%f').timestamp()
					while t1_ < t2_:	
#						print(t)		
						data = self._tables[t].select_table(datetime.datetime.fromtimestamp(t1_), datetime.datetime.fromtimestamp(t1_+ delta[1]), chans)
#						print(delta[1])

						d = defaultdict(list)
						for i in data:
							d[i.ch_id].append(i)	
						compress1 = Average(t1_, delta, d, self._tables[t],delta[0])
						compress2 = DataSampler(t1_, delta, d, self._tables[t],delta[0])
						t1_+=delta[1]	
					sesion_compress.commit()

		return True	

	def processed_data3(self, t1, t2, dict_interval, channels=None):
		
		ch = Channels_()
		table_channels = ch.get_channels(self.session)
		dict_channels  = ch.found_type(table_channels, channels)
		for t, chans in dict_channels.items():
			if t!="event":			
					t1_ = datetime.datetime.strptime(t1,'%Y-%m-%d %H:%M:%S.%f').timestamp()
					t2_ = datetime.datetime.strptime(t2,'%Y-%m-%d %H:%M:%S.%f').timestamp()
					delta_extract = dict_interval["third_level"]
					while t1_ < t2_:		
						data = self._tables[t].select_table(datetime.datetime.fromtimestamp(t1_), datetime.datetime.fromtimestamp(t1_+ delta_extract), chans)
						#print(delta)
						d = defaultdict(list)
						for i in data:
							d[i.ch_id].append(i)
							
						for name, delta in dict_interval.items():
							for ch, arr in d.items():
								time = t1_
								step = -1
								while time < t1_+delta_extract:
									dict_result = defaultdict(list)
									for i in arr[step+1:]:
										if i.time.timestamp() <= time + delta:
											step=arr.index(i)
											dict_result[i.ch_id].append(i)
										

									compress1 = Average(time, delta, dict_result, self._tables[t], name)
									compress2 = DataSampler(time, delta, dict_result, self._tables[t], name)
									time+=delta
							sesion_compress.commit()
										
						t1_+=delta_extract						

	def get_processed_data(self, t1, t2, channels, fields, compress_level):
		delta = []
		ch = Channels_()
		table_channels = ch.get_channels(Session())
		dict_channels = ch.found_type(table_channels, channels)
		f = 0
		array = []
		d = defaultdict(list)
		result = defaultdict(list)
		for i in fields:
			for c_method, field in self.dict_fields.items():
				for f in field:
					if i == f:
						d[c_method].append(i)
						
		print(t1,t2,fields)
		for t, ch in dict_channels.items():
			chan = []
			for x in ch:			
				chan.append(x["id"])
			for row in self.session.query(self.dict_table[compress_level][t][fields]).filter(self.dict_table[compress_level][t][fields].time>=t1, 
																										self.dict_table[compress_level][t][fields].time<=t2, 
																										self.dict_table[compress_level][t][fields].ch_id.in_(chan)):						
					if fields=="average":
						result[x["name"]].append({"time": datetime.datetime.strftime(row.time1,'%Y-%m-%d %H:%M:%S.%f'), "avg":row.avg, "min":row.min, "max":row.max, "mediana":row.mediana, "sygma":row.sygma})
					elif fields=="data_sampler":
						result[x["name"]].append({"time": datetime.datetime.strftime(row.time,'%Y-%m-%d %H:%M:%S.%f'), "left_bound": row.left_bound, "right_bound":row.right_bound, "center":row.center, "most_freq":row.most_freq})
		return result



if __name__ =='__main__':
	import cProfile
	proces = Process(Session)
	fields = ["min", "max", "center", "sygma"]
	dict_interval = {"first_level" : 120.0, "second_level" : 300.0, "third_level" : 600.0}
	session = Session()
	proces.processed_data3("2013-04-27 00:00:00.00000", "2013-04-27 01:00:00.00000", dict_interval)
