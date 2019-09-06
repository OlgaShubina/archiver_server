from __future__ import absolute_import, unicode_literals
from celery import Celery
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from compress import Process
from celery.utils.log import get_task_logger
from celery.schedules import crontab
import redis
from celery.decorators import periodic_task
import datetime
from parsconfig import Config
from table import Archive

config = Config('config.yaml')

connect = redis.StrictRedis(host=config.storage["host"], port=config.storage["port"], db=config.storage["demon_db"])

app = Celery('task', backend=config.celery["backend"], broker=config.celery["broker"],
             result_backend=config.celery["result_backend"])
url = 'postgresql://{}:{}@{}:{}/{}'.format(config.archive["database"]["user"],
                                           config.archive["database"]["password"], config.archive["database"]["host"],
                                           config.archive["database"]["port"], config.archive["database"]["dbname"])

engine = create_engine(url, client_encoding='utf8')
Session = sessionmaker(bind=engine)

proces = Process(Session)
archive = Archive(Session)

dict_interval = config.main["dict_interval"]

app.conf.timezone = 'UTC'

logger = get_task_logger(__name__)


@app.task(bind=True)
def add_value(self, t1, t2, dict_interval):
    print(t1, t2)
    r = proces.processed_data3(datetime.datetime.fromtimestamp(t1).strftime('%Y-%m-%d %H:%M:%S.%f'),
                               datetime.datetime.fromtimestamp(t2).strftime('%Y-%m-%d %H:%M:%S.%f'),
                               dict_interval)
    return (self.request.id)


@periodic_task(run_every=crontab(minute=0, hour=12))
def clear_storage():
    array = []
    for i in connect.keys():
        if i.decode("utf-8") != "last_time":
            array.append(i)
    for i in array:
        r = connect.hgetall("{}".format(i.decode("utf-8")))
        if app.AsyncResult(i.decode("utf-8")).ready() == True:
            connect.delete(i.decode("utf-8"))
        else:
            archive.delete_raw((datetime.datetime.strptime(r[b't1'].decode("utf-8"), '%Y-%m-%d %H:%M:%S')).timestamp(),
                               (datetime.datetime.strptime(r[b't2'].decode("utf-8"), '%Y-%m-%d %H:%M:%S')).timestamp())

            add_value.delay((datetime.datetime.strptime(r[b't1'].decode("utf-8"), '%Y-%m-%d %H:%M:%S')).timestamp(),
                            (datetime.datetime.strptime(r[b't2'].decode("utf-8"), '%Y-%m-%d %H:%M:%S')).timestamp(),
                            dict_interval)


@periodic_task(run_every=crontab(minute='*/10'))
def process():
    try:
        t = archive.get_last_time()
        delta = datetime.timedelta(seconds=config.celery["interval_add_value"], microseconds=0.0)
        t1 = t + delta
        t2 = t1 + datetime.timedelta(minutes=config.celery["interval_process_data"], microseconds=0.0)
        logger.debug("%s %s" % (t1, t2))
        while t1 < t2:
            logger.debug("%s %s" % (t1, t1 + delta))
            r = add_value.delay(t1.timestamp(), (t1 + delta).timestamp(), dict_interval)
            dict_process = {"t1": t1, "t2": t1}
            connect.hmset("{}".format(r), dict_process)
            t1 += delta

    except Exception as exc:
        logger.debug("%s" % (exc))
        raise self.retry(exc=exc, countdown=10)
