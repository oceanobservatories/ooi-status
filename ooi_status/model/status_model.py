"""
Track the CI particles being ingested into cassandra and store information into Postgres.

@author Dan Mergens
"""
from collections import OrderedDict

from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class ReferenceDesignator(Base):
    __tablename__ = 'reference_designator'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)

    def asdict(self):
        fields = ['id', 'name']
        return {field: getattr(self, field) for field in fields}

    def __repr__(self):
        return '{0}'.format(self.name)


class ExpectedStream(Base):
    __tablename__ = 'expected_stream'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    method = Column(String, nullable=False)
    rate = Column(Float, default=0)  # 0 means untracked
    warn_interval = Column(Integer, default=0)  # 0 means untracked
    fail_interval = Column(Integer, default=0)  # 0 means untracked

    def asdict(self):
        fields = ['id', 'name', 'method', 'rate', 'warn_interval', 'fail_interval']
        return {field: getattr(self, field) for field in fields}

    def __repr__(self):
        return '{0} {1} {2} Hz {3}/{4}'.format(self.name, self.method, self.rate, self.warn_interval, self.fail_interval)


class DeployedStream(Base):
    __tablename__ = 'deployed_stream'
    id = Column(Integer, primary_key=True)
    ref_des_id = Column(Integer, ForeignKey('reference_designator.id'), nullable=False)
    ref_des = relationship(ReferenceDesignator, backref='deployed_streams', lazy='joined')
    expected_stream_id = Column(Integer, ForeignKey('expected_stream.id'), nullable=False)
    expected_stream = relationship(ExpectedStream, backref='deployed_streams', lazy='joined')

    def asdict(self):
        fields = ['id', 'ref_des', 'expected_stream']
        return {field: getattr(self, field) for field in fields}

    def last_seen(self, session):
        last_count = session.query(Counts).filter(Counts.stream == self).order_by(Counts.timestamp.desc()).first()
        if last_count:
            return last_count.timestamp
        else:
            return datetime.utcfromtimestamp(0)

    def __repr__(self):
        return '{0} {1}'.format(self.ref_des, self.expected_stream)


class Counts(Base):
    __tablename__ = 'counts'
    id = Column(Integer, primary_key=True)
    stream_id = Column(Integer, ForeignKey('deployed_stream.id'), nullable=False)
    stream = relationship(DeployedStream, backref='counts')
    particle_count = Column(Integer, nullable=False)
    timestamp = Column(DateTime, nullable=False)

    def rate(self, count):
        return abs((count.particle_count - self.particle_count) / (count.timestamp - self.timestamp).seconds)

    def __repr__(self):
        return '{0} {1} particles at {2}'.format(self.stream, self.particle_count, self.timestamp)


def create_database(engine, drop=False):
    if drop:
        Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
