
import parsconfig
from collections import namedtuple, defaultdict
import math
import psycopg2
import datetime
import math
import time

LogDouble = namedtuple('LogDouble','time, ch_id, value')


class Average(object):
	"""docstring for Average"""
	def __init__(self, config):
		self.conn = psycopg2.connect("dbname={} user={} password={} host={}". format("logger", "logger", "logger", "127.0.0.1"))
		self.query_select = "SELECT * FROM {} WHERE time BETWEEN CAST('{}' AS TIMESTAMP) AND CAST('{}' AS TIMESTAMP)"
		self.query_insert = "INSERT INTO {} (time1, time2, ch_id, avg, mediana, max, min, sygma) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
	def sort_value(self, value):
		return value

	def get_data(self, t1, t2):
		t = time.time()
		with self.conn.cursor() as cur:
			cur.execute(self.query_select.format("log_double", t1, t2))
			data = [LogDouble(*row) for row in cur]
		return data

	
	def sum_data(self, data):
		return sum(data)

	def median_data(self, value, lenght):
		value.sort()
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

	def min_data(self, value):
		if len(value) == 0:
			return 0
		else:		
			return min(value)

	def max_data(self, value):
		if len(value) == 0:
			return 0
		else:			
			return max(value)

	def sygma(self, value, lenght, average):
		summa = 0
		for i in value:
			summa += (i + average)**2
		sygma = math.sqrt(summa/lenght)
		return sygma

	def insert_data(self, time1, time2, ch_id, avg, mediana, min, max, syg):
		cur = self.conn.cursor()
		cur.execute(self.query_insert.format("average"), (time1, time2, ch_id, avg, mediana, max, min, syg))

	def compress_data(self, t1, t2, delta):
		t1_ = datetime.datetime.strptime(t1,'%Y-%m-%d %H:%M:%S.%f').timestamp()
		t2_ = datetime.datetime.strptime(t2,'%Y-%m-%d %H:%M:%S.%f').timestamp()
		
		while t1_ < t2_:			
			data = self.get_data(datetime.datetime.fromtimestamp(t1_), datetime.datetime.fromtimestamp(t1_+delta))
			d = defaultdict(list)
			for i in data:
				d[i.ch_id].append(i)
			for ch, i in d.items():
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
						mediana = obj.median_data(l, lenght)
						minimum = l[0]
						maximum = l[-1]
					ll = list(filter(lambda x: not math.isinf(x), l))
					lenght = len(ll)
					if lenght == 0:
						syg = None	
						avg = None
					else:
						avg = obj.sum_data(ll)/lenght					
						syg = obj.sygma(ll, lenght, avg)

				self.insert_data(datetime.datetime.fromtimestamp(t1_), datetime.datetime.fromtimestamp(t1_ + delta), ch, avg, mediana, minimum, maximum, syg)
			self.conn.commit()
			t1_+=delta
		

if __name__ =='__main__':
	import cProfile

	config = parsconfig.Config('/home/olga/projects/config.yaml')
	obj = Average(config.config["pg_archive"])
	def compress():	
		obj.compress_data("2013-04-27 00:00:00.00000","2013-04-27 00:01:59.00000", 120.0)

	cProfile.run('compress()')