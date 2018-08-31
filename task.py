from __future__ import absolute_import, unicode_literals
from celery import Celery
from celery import task 
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from compress import Process
from celery.utils.log import get_task_logger
import simplejson as json
from time import sleep
from celery.schedules import crontab
import redis
import datetime
from parsconfig import Config


config = Config('config.yaml')

connect = redis.StrictRedis(host=config.storage["host"], port=config.storage["port"], db=config.storage["demon_db"])

app = Celery('task', backend = config.celery["backend"] , broker = config.celery["broker"], result_backend = config.celery["result_backend"])
url = 'postgresql://{}:{}@{}:{}/{}'.format(config.archive["database"]["user"], config.archive["database"]["password"], config.archive["database"]["host"], config.archive["database"]["port"], config.archive["database"]["dbname"])
engine = create_engine(url, client_encoding='utf8') 
Session = sessionmaker(bind=engine)
proces = Process(Session)

dict_interval = config.main["dict_interval"]


app.conf.timezone = 'UTC'

logger = get_task_logger(__name__)

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
	# Calls test('hello') every 10 seconds.
	
	r = sender.add_periodic_task(120.0, process.s("add_value"), name='add value')
	r = sender.add_periodic_task(240.0, process.s("clear_storage"), name='clear_ctorage')
	#result = add.AsyncResult(add.id).state
	
	#print(r.id)

@app.task(bind=True)
def add_value(self, t1, t2, dict_interval):

	print(t1, t2)
	r = proces.processed_data3(datetime.datetime.fromtimestamp(t1).strftime('%Y-%m-%d %H:%M:%S.%f'), 
								datetime.datetime.fromtimestamp(t2).strftime('%Y-%m-%d %H:%M:%S.%f'), 
								dict_interval)
	return(self.request.id)
	
@app.task(bind=True)
def clear_storage(self):
	array = []
	for i in connect.keys():
		if i.decode("utf-8")!="last_time":
			array.append(i)
	for i in array:
		r = connect.hgetall("{}".format(i.decode("utf-8")))
		if app.AsyncResult(i.decode("utf-8")).ready() == True:
			connect.delete(i.decode("utf-8"))
			print("true")
		else:
			print("false")
			add_value.delay((datetime.datetime.strptime(r[b't1'].decode("utf-8"),'%Y-%m-%d %H:%M:%S')).timestamp(), 
							(datetime.datetime.strptime(r[b't2'].decode("utf-8"),'%Y-%m-%d %H:%M:%S')).timestamp(), 
							dict_interval)
			
	
	


@app.task(bind=True)
def process(self, funk):
	try:		
		if(funk=="add_value"):
			print("add_value")
			t = connect.get("last_time").decode("utf-8")
			print(t)
			t1 = (datetime.datetime.strptime(t,'%Y-%m-%d %H:%M:%S'))
			t2 = t1 + datetime.timedelta(seconds=960, microseconds=0.0)
			connect.delete("last_time")
			connect.set("last_time", t2.strftime('%Y-%m-%d %H:%M:%S'))
			r = add_value.delay(t1.timestamp(), t2.timestamp(), dict_interval)
			dict_process = {"t1" : t1, "t2" : t2}
			connect.hmset("{}".format(r), dict_process)
		else:
			clear_storage.delay()
		
		

	except Exception as exc:
		 raise self.retry(exc=exc, countdown=10)
