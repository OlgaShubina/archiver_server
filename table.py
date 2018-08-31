from abc import ABCMeta, abstractmethod
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, PrimaryKeyConstraint
import sqlalchemy
import datetime
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from collections import namedtuple, defaultdict
from channels import Channels_
from process_table import CompressDouble,  DSDouble, DSText,  DSDoubleSecond,  DSDoubleThird,  CompressDoubleSecond, CompressDoubleThird, DSTextSecond, DSTextThird
from parsconfig import Config
from sqlalchemy.orm import mapper
from sqlalchemy import inspect
from sqlalchemy.ext.automap import automap_base

Base = declarative_base()
config = Config('config.yaml')
url = 'postgresql://{}:{}@{}:{}/{}'.format(config.archive["database"]["user"], config.archive["database"]["password"], config.archive["database"]["host"], config.archive["database"]["port"], config.archive["database"]["dbname"])

engine = sqlalchemy.create_engine(url, client_encoding='utf8') 
Session = sessionmaker(bind=engine)

url_compress = 'postgresql://{}:{}@{}:{}/{}'.format(config.compress_archive["database"]["user"], config.compress_archive["database"]["password"], config.compress_archive["database"]["host"], config.compress_archive["database"]["port"], config.compress_archive["database"]["dbname"])
engine_compress = sqlalchemy.create_engine(url_compress, client_encoding='utf8') 
Session_compress = sessionmaker(bind=engine_compress)
sesion_compress = Session_compress()

inspector = inspect(engine)
metadata = MetaData()
metadata.reflect(engine, only=config.archive["table_name"])
i = 0
for n in config.archive["tables"]:
	pk = PrimaryKeyConstraint(config.archive["time_field"], config.archive["id_field"], name='mytable_pk'+ str(i))
	metadata.tables[n["name"]].append_constraint(pk)
	Base = automap_base(metadata=metadata)
	Base.prepare()

LogDouble = Base.classes.log_double
LogText = Base.classes.log_text


class Table(object):
	"""docstring for Table"""
	def __init__(self, name, Session):
		self._session = Session
		self.name = name

	def select_table(self, t1, t2, channels=None):
		array = []
		if channels == None:
			for instance in self._session.query(self.name).filter(getattr(self.name, config.archive["time_field"]) >= t1, getattr(self.name, config.archive["time_field"]) <= t2):
				array.append(instance)
		else:
			if isinstance(channels, list):
				ch = []
				for i in channels:
						ch.append(i["id"])
				for instance in self._session.query(self.name).filter(getattr(self.name, config.archive["time_field"]) >= t1, 
																		getattr(self.name, config.archive["time_field"]) <= t2, 
																		getattr(self.name, config.archive["id_field"]).in_(ch)):
					array.append(instance)
			else:
				for instance in self._session.query(self.name).filter(getattr(self.name, config.archive["time_field"]) >= t1,
																		getattr(self.name, config.archive["time_field"]) <= t2,
																		getattr(self.name, config.archive["id_field"]) == channels.id):
					array.append(instance)
		return array

	def insert_table(self, table):
		self._session.add(table)
		
class Archive(object):
	"""docstring for Table"""
	def __init__(self, Session):
		self._tables = {}
		self.session = Session()
		self.sesion_compress = Session_compress()
		self.tables_arr = [CompressDouble, DSDouble, DSText, CompressDoubleSecond, DSDoubleSecond, DSTextSecond, CompressDoubleThird, DSDoubleThird, DSTextThird]
		self._mapping = { "raw": {'numeric': LogDouble, 'text' : LogText},
							"first_level" : {"numeric" : {"average" : CompressDouble, "data_sampler" : DSDouble}, "text" : {"data_sampler" : DSText}}, 
							"second_level" : {"numeric" : {"average" : CompressDoubleSecond, "data_sampler" : DSDoubleSecond}, "text" : {"data_sampler" : DSTextSecond}},
							"third_level" : {"numeric" :{"average" : CompressDoubleThird, "data_sampler" : DSDoubleThird}, "text" : {"data_sampler" : DSTextThird}}}		
		for compress_level, table_dict in self._mapping.items():
			temp_dict = {}
			if(compress_level=="raw"):
				for table_type, table_name in table_dict.items():
					temp_dict[table_type] = Table(table_name, self.session)
				self._tables[compress_level] = temp_dict	
			else:
				for table_type, dict_method in table_dict.items():
					temp2_dict = {}
					for process_method, table_name in dict_method.items():
						temp2_dict[process_method] = Table(table_name, self.sesion_compress)
					temp_dict[table_type] = temp2_dict
				self._tables[compress_level] = temp_dict	

	def delete_row(self, t1, t2):
		for table in self.tables_arr:
			current_time = self.sesion_compress.query(CompressDoubleThird).order_by(CompressDoubleThird.time.desc()).first().time
			if current_time != t1:	
				self.sesion_compress.query(table).filter(table.time >= t1, table.time <= t2).delete()

	def get_last_time(self):
		current_time = self.sesion_compress.query(CompressDoubleThird).order_by(CompressDoubleThird.time.desc()).first().time
		return current_time

	def get_data(self, t1, t2, channels, compress_level, process_method=None): 
		data = []        
		ch = Channels_()
		table_channels = ch.get_channels(Session())
		result_dict = defaultdict(list)
		d = ch.found_type(table_channels, channels) 
		print(d)      
		for t, chans in d.items():
			if(compress_level=="raw"):
				data += self._tables[compress_level][t].select_table(t1, t2, chans)
				for i in data:
					for k in chans:
						if(k["id"]==i.ch_id):
							result_dict[k["name"]].append({"time" : datetime.datetime.strftime(i.time,'%Y-%m-%d %H:%M:%S.%f'), "value" : i.value})
			else:
				data += self._tables[compress_level][t][process_method].select_table(t1, t2, chans)
				if(process_method=="average"):
					for row in data:						
						for k in chans:
							if(k["id"]==row.ch_id):
								result_dict[k["name"]].append({"time": datetime.datetime.strftime(row.time,'%Y-%m-%d %H:%M:%S.%f'), "avg":row.avg, "min":row.min, "max":row.max, "mediana":row.mediana, "sigma":row.sygma})
				else:
					for row in data:
						for k in chans:
							if(k["id"]==row.ch_id):
								result_dict[k["name"]].append({"time": datetime.datetime.strftime(row.time,'%Y-%m-%d %H:%M:%S.%f'), "left_bound": row.left_bound, "right_bound":row.right_bound, "center":row.center, "most_freq":row.most_freq})
					
		return result_dict

	