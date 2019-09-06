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
from channels import Channels_
import numpy as np
from parsconfig import Config

config = Config('/home/olga/PycharmProjects/rest_api/config.yaml')

Base = declarative_base()
url = 'postgresql://{}:{}@{}:{}/{}'.format(config.compress_archive["database"]["user"],
                                           config.compress_archive["database"]["password"],
                                           config.compress_archive["database"]["host"],
                                           config.compress_archive["database"]["port"],
                                           config.compress_archive["database"]["dbname"])
engine = sqlalchemy.create_engine(url, client_encoding='utf8')
Session = sessionmaker(bind=engine)
metadata = Base.metadata


class DSText(Base):
    """docstring for CompressText"""
    __tablename__ = "ds_text"
    time = Column(postgresql.TIMESTAMP(timezone=True), primary_key=True)
    ch_id = Column(Integer, primary_key=True)
    left_bound = Column(String(1024))
    right_bound = Column(String(1024))
    most_freq = Column(String(1024))
    center = Column(String(1024))

    def __init__(self, time, delta, ch_id, left_bound, right_bound, most_freq, center):
        self.time = time
        self.ch_id = ch_id
        self.left_bound = left_bound
        self.right_bound = right_bound
        self.most_freq = most_freq
        self.center = center


class DSTextSecond(Base):
    """docstring for CompressText"""
    __tablename__ = "ds_text_second"
    time = Column(postgresql.TIMESTAMP(timezone=True), primary_key=True)
    ch_id = Column(Integer, primary_key=True)
    left_bound = Column(String(1024))
    right_bound = Column(String(1024))
    most_freq = Column(String(1024))
    center = Column(String(1024))

    def __init__(self, time, delta, ch_id, left_bound, right_bound, most_freq, center):
        self.time = time
        self.ch_id = ch_id
        self.left_bound = left_bound
        self.right_bound = right_bound
        self.most_freq = most_freq
        self.center = center


class DSTextThird(Base):
    """docstring for CompressText"""
    __tablename__ = "ds_text_third"
    time = Column(postgresql.TIMESTAMP(timezone=True), primary_key=True)
    ch_id = Column(Integer, primary_key=True)
    left_bound = Column(String(1024))
    right_bound = Column(String(1024))
    most_freq = Column(String(1024))
    center = Column(String(1024))

    def __init__(self, time, delta, ch_id, left_bound, right_bound, most_freq, center):
        self.time = time
        self.ch_id = ch_id
        self.left_bound = left_bound
        self.right_bound = right_bound
        self.most_freq = most_freq
        self.center = center


class DSDouble(Base):
    """docstring for CompressText"""
    __tablename__ = "ds_double"
    time = Column(postgresql.TIMESTAMP(timezone=True), primary_key=True)
    ch_id = Column(Integer, primary_key=True)
    left_bound = Column(postgresql.DOUBLE_PRECISION)
    right_bound = Column(postgresql.DOUBLE_PRECISION)
    most_freq = Column(postgresql.DOUBLE_PRECISION)
    center = Column(postgresql.DOUBLE_PRECISION)

    def __init__(self, time, delta, ch_id, left_bound, right_bound, most_freq, center):
        self.time = time
        self.ch_id = ch_id
        self.left_bound = left_bound
        self.right_bound = right_bound
        self.most_freq = most_freq
        self.center = center


class DSDoubleSecond(Base):
    """docstring for CompressText"""
    __tablename__ = "ds_double_second"
    time = Column(postgresql.TIMESTAMP(timezone=True), primary_key=True)
    ch_id = Column(Integer, primary_key=True)
    left_bound = Column(postgresql.DOUBLE_PRECISION)
    right_bound = Column(postgresql.DOUBLE_PRECISION)
    most_freq = Column(postgresql.DOUBLE_PRECISION)
    center = Column(postgresql.DOUBLE_PRECISION)

    def __init__(self, time, delta, ch_id, left_bound, right_bound, most_freq, center):
        self.time = time
        self.ch_id = ch_id
        self.left_bound = left_bound
        self.right_bound = right_bound
        self.most_freq = most_freq
        self.center = center


class DSDoubleThird(Base):
    """docstring for CompressText"""
    __tablename__ = "ds_double_third"
    time = Column(postgresql.TIMESTAMP(timezone=True), primary_key=True)
    ch_id = Column(Integer, primary_key=True)
    left_bound = Column(postgresql.DOUBLE_PRECISION)
    right_bound = Column(postgresql.DOUBLE_PRECISION)
    most_freq = Column(postgresql.DOUBLE_PRECISION)
    center = Column(postgresql.DOUBLE_PRECISION)

    def __init__(self, time, delta, ch_id, left_bound, right_bound, most_freq, center):
        self.time = time
        self.ch_id = ch_id
        self.left_bound = left_bound
        self.right_bound = right_bound
        self.most_freq = most_freq
        self.center = center


class CompressDouble(Base):
    """docstring for CompressDouble"""
    __tablename__ = "average"
    time = Column(postgresql.TIMESTAMP(timezone=True), primary_key=True)
    ch_id = Column(Integer, primary_key=True)
    avg = Column(postgresql.DOUBLE_PRECISION)
    mediana = Column(postgresql.DOUBLE_PRECISION)
    max = Column(postgresql.DOUBLE_PRECISION)
    min = Column(postgresql.DOUBLE_PRECISION)
    sygma = Column(postgresql.DOUBLE_PRECISION)

    def __init__(self, time, delta, ch_id, avg, mediana, min, max, syg):
        self.time = time
        self.ch_id = ch_id
        self.avg = avg
        self.mediana = mediana
        self.min = min
        self.max = max
        self.sygma = syg


class CompressDoubleSecond(Base):
    """docstring for CompressDouble"""
    __tablename__ = "average_second"
    time = Column(postgresql.TIMESTAMP(timezone=True), primary_key=True)
    ch_id = Column(Integer, primary_key=True)
    avg = Column(postgresql.DOUBLE_PRECISION)
    mediana = Column(postgresql.DOUBLE_PRECISION)
    max = Column(postgresql.DOUBLE_PRECISION)
    min = Column(postgresql.DOUBLE_PRECISION)
    sygma = Column(postgresql.DOUBLE_PRECISION)

    def __init__(self, time, delta, ch_id, avg, mediana, min, max, syg):
        self.time = time
        self.ch_id = ch_id
        self.avg = avg
        self.mediana = mediana
        self.min = min
        self.max = max
        self.sygma = syg


class CompressDoubleThird(Base):
    """docstring for CompressDouble"""
    __tablename__ = "average_third"
    time = Column(postgresql.TIMESTAMP(timezone=True), primary_key=True)
    ch_id = Column(Integer, primary_key=True)
    avg = Column(postgresql.DOUBLE_PRECISION)
    mediana = Column(postgresql.DOUBLE_PRECISION)
    max = Column(postgresql.DOUBLE_PRECISION)
    min = Column(postgresql.DOUBLE_PRECISION)
    sygma = Column(postgresql.DOUBLE_PRECISION)

    def __init__(self, time, delta, ch_id, avg, mediana, min, max, syg):
        self.time = time
        self.ch_id = ch_id
        self.avg = avg
        self.mediana = mediana
        self.min = min
        self.max = max
        self.sygma = syg


metadata.create_all(engine)
