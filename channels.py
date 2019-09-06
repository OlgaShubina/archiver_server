from abc import ABCMeta, abstractmethod
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
import sqlalchemy
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from collections import namedtuple, defaultdict
import parsconfig

Base = declarative_base()
config = parsconfig.Config('/home/olga/PycharmProjects/rest_api/config.yaml')
url = 'postgresql://{}:{}@{}:{}/{}'.format(config.archive["database"]["user"], config.archive["database"]["password"], config.archive["database"]["host"], config.archive["database"]["port"], config.archive["database"]["dbname"])
engine = sqlalchemy.create_engine(url, client_encoding='utf8') 
Session = sessionmaker(bind=engine)


class Channels(Base):
    """docstring for Channels"""
    __tablename__ = "channels"
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    units = Column(String(255))
    threshold = Column(sqlalchemy.types.Numeric)
    is_log = Column(sqlalchemy.types.Boolean)
    description = Column(sqlalchemy.types.Text)
    cas_type  = Column(String(255))
    log_type = Column(String(255))

    def __init__(self, id, name, units, threshold, is_log, description, cas_type, log_type):
        self.id = id
        self.name = name
        self.units = units
        self.threshold = threshold
        self.is_log = is_log
        self.description = description
        self.cas_type = cas_type
        self.log_type = log_type

class Channels_(object):
    """docstring for Channels_"""
    def get_channels(self, session):
        session2 = Session()
        table_channels = []
        for instance in session2.query(Channels):
            table_channels.append(instance)
        if table_channels is None:
            logging.error("table not created")
        return table_channels

    def found_type(self, table_channels, channels=None):
        d = defaultdict(list)
        array = []
        if channels != None:
            for row in table_channels:
                if isinstance(channels, list):
                    for i in channels:
                        if (row.id == i) or (row.name == i):
                           array.append(row)

                          # print(row.name)
                else:
                    if (row.id == channels) or (row.name == channels):
                        array.append(row)
            for ch in array:
                d[ch.log_type].append({"id": ch.id, "name": ch.name})
        else:
            for ch in table_channels:
                d[ch.log_type].append({"id": ch.id, "name": ch.name})
        return d

    def return_tree(self, array_channels):
        d = defaultdict(list)
        dd = defaultdict(list)

        for i in array_channels:
            d[i["name"]].append(i)
        for i,k in sorted(d.items()):
            index = i.rfind("/")
            if index != -1:
                dd[i[:index]].append(k)
            else:
                dd.update({i:k})
        d.clear()
        for i,k in sorted(dd.items()):
            index = i.rfind("/")
            if index != -1:
                d[i[:index]].append({i[index+1:]:k})
            else:
                d.update({i:k})
        dd.clear()
        for i,k in sorted(d.items()):
            index = i.rfind("/")
            if index != -1:
                dd[i[:index]].append({i[index+1:]:k})
            else:
                dd.update({i:k})
        d.clear()
        for i,k in sorted(dd.items()):
            index = i.rfind("/")
            if index != -1:
                d[i[:index]].append({i[index+1:]:k})
            else:
                d.update({i:k})
        dd.clear()
        for i,k in sorted(d.items()):
            index = i.rfind("/")
            if index != -1:
                dd[i[:index]].append({i[index+1:]:k})
            else:
                dd.update({i:k})
        return dd