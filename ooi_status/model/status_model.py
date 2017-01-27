"""
Track the CI particles being ingested into cassandra and store information into Postgres.

@author Dan Mergens
"""
import datetime

import logging

from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import relationship

from ooi_status.get_logger import get_logger
from .base import MonitorBase


log = get_logger(__name__, logging.INFO)


class ReferenceDesignator(MonitorBase):
    __tablename__ = 'reference_designator'
    __table_args__ = (
        UniqueConstraint('name'),
    )
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

    def __repr__(self):
        return self.name

    @staticmethod
    def get_or_create(session, reference_designator):
        object = session.query(ReferenceDesignator).filter(
            ReferenceDesignator.name == reference_designator).first()
        if object is None:
            object = ReferenceDesignator(name=reference_designator)
            session.add(object)
            session.flush()
        return object


class ExpectedStream(MonitorBase):
    __tablename__ = 'expected_stream'
    __table_args__ = (
        UniqueConstraint('name', 'method'),
    )
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    method = Column(String, nullable=False)
    expected_rate = Column(Float, default=0, nullable=False)
    warn_interval = Column(Integer, default=0, nullable=False)
    fail_interval = Column(Integer, default=0, nullable=False)

    @staticmethod
    def get_or_create(session, stream, method):
        expected = session.query(ExpectedStream).filter(ExpectedStream.name == stream,
                                                        ExpectedStream.method == method).first()
        if expected is None:
            log.info('Creating ExpectedStream(name=%s, method=%s', stream, method)
            expected = ExpectedStream(name=stream, method=method)
            session.add(expected)
            session.flush()
        return expected

    def asdict(self):
        fields = ['id', 'name', 'method', 'expected_rate', 'warn_interval', 'fail_interval']
        return {field: getattr(self, field) for field in fields}

    def __repr__(self):
        return '{0} {1} {2} Hz {3}/{4}'.format(self.name, self.method, self.expected_rate,
                                               self.warn_interval, self.fail_interval)


class DeployedStream(MonitorBase):
    __tablename__ = 'deployed_stream'
    __table_args__ = (
        UniqueConstraint('reference_designator_id', 'expected_stream_id'),
    )
    id = Column(Integer, primary_key=True)
    reference_designator_id = Column(Integer, ForeignKey('reference_designator.id'), nullable=False)
    expected_stream_id = Column(Integer, ForeignKey('expected_stream.id'), nullable=False)
    particle_count = Column(Integer, nullable=False)
    last_seen = Column(DateTime, nullable=False)
    collected = Column(DateTime, nullable=False)
    _expected_rate = Column('expected_rate', Float)
    _warn_interval = Column('warn_interval', Integer)
    _fail_interval = Column('fail_interval', Integer)
    reference_designator = relationship(ReferenceDesignator, backref='deployed_streams', lazy='joined')
    expected_stream = relationship(ExpectedStream, backref='deployed_streams', lazy='joined')
    stream_condition = relationship('StreamCondition', uselist=False, back_populates="deployed_stream")

    @staticmethod
    def get_or_create(session, refdes_obj, expected_obj, count, timestamp, coll_time):
        deployed = session.query(DeployedStream).filter(DeployedStream.reference_designator == refdes_obj,
                                                        DeployedStream.expected_stream == expected_obj).first()
        if deployed is None:
            deployed = DeployedStream(reference_designator=refdes_obj, expected_stream=expected_obj,
                                      particle_count=count, last_seen=timestamp, collected=coll_time)
            session.add(deployed)
            session.flush()
        return deployed

    def asdict(self):
        return {
            'id': self.id,
            'reference_designator': self.reference_designator.name,
            'reference_designator_id': self.reference_designator.id,
            'expected_stream': self.expected_stream,
            'expected_rate': self._expected_rate,
            'warn_interval': self._warn_interval,
            'fail_interval': self._fail_interval,
        }

    def __repr__(self):
        return '{0} {1} {2} {3}'.format(self.reference_designator, self.expected_stream,
                                        self.collected, self.particle_count)

    def get_status(self, elapsed):
        elapsed_seconds = elapsed.total_seconds()
        if self.untracked:
            return None, None
        if elapsed_seconds > self.fail_interval:
            return 'failed', datetime.timedelta(seconds=self.fail_interval)
        if elapsed_seconds > self.warn_interval:
            return 'degraded', datetime.timedelta(seconds=self.warn_interval),
        return 'operational', datetime.timedelta(seconds=self.warn_interval)

    @property
    def expected_rate(self):
        return self.expected_stream.expected_rate if self._expected_rate is None else self._expected_rate

    @property
    def warn_interval(self):
        return self.expected_stream.warn_interval if self._warn_interval is None else self._warn_interval

    @property
    def fail_interval(self):
        return self.expected_stream.fail_interval if self._fail_interval is None else self._fail_interval

    @property
    def untracked(self):
        return all((
            self.expected_rate == 0,
            self.warn_interval == 0,
            self.fail_interval == 0
        ))

    @property
    def disabled(self):
        return all((
            self._expected_rate == 0,
            self._warn_interval == 0,
            self._fail_interval == 0
        ))

    def disable(self):
        self._expected_rate = 0
        self._warn_interval = 0
        self._fail_interval = 0

    def enable(self):
        self._expected_rate = None
        self._warn_interval = None
        self._fail_interval = None


class StreamCondition(MonitorBase):
    __tablename__ = 'stream_condition'
    __table_args__ = (
        UniqueConstraint('stream_id'),
    )
    id = Column(Integer, primary_key=True)
    stream_id = Column(Integer, ForeignKey('deployed_stream.id'), nullable=False)
    last_status_time = Column(DateTime, nullable=False)
    last_status = Column(String, nullable=False)

    deployed_stream = relationship('DeployedStream', back_populates='stream_condition')


class StreamCount(MonitorBase):
    __tablename__ = 'stream_count'
    id = Column(Integer, primary_key=True)
    stream_id = Column(Integer, ForeignKey('deployed_stream.id'), nullable=False)
    collected_time = Column(DateTime, nullable=False)
    particle_count = Column(Integer, default=0)
    seconds = Column(Float, default=0)
    stream = relationship(DeployedStream, backref='stream_counts')

    def __repr__(self):
        return 'StreamCount(stream_id=%d, count=%d, seconds=%f, rate=%f, collected=%s)' % (
            self.stream_id,
            self.particle_count,
            self.seconds,
            self.particle_count / self.seconds,
            self.collected_time
        )


class PortCount(MonitorBase):
    __tablename__ = 'port_count'
    id = Column(Integer, primary_key=True)
    reference_designator_id = Column(Integer, ForeignKey('reference_designator.id'), nullable=False)
    collected_time = Column(DateTime, nullable=False)
    byte_count = Column(Integer, default=0)
    seconds = Column(Float, default=0)
    reference_designator = relationship(ReferenceDesignator, backref='port_counts')


class NotifyAddress(MonitorBase):
    __tablename__ = 'notify_address'
    __table_args__ = (
        UniqueConstraint('email_addr', 'email_type'),
    )
    id = Column(Integer, primary_key=True)
    email_addr = Column(String, nullable=False)
    email_type = Column(String, nullable=False)

    def asdict(self):
        return {'addr': self.email_addr, 'type': self.email_type}


class PendingUpdate(MonitorBase):
    __tablename__ = 'pending_update'
    id = Column(Integer, primary_key=True)
    message = Column(JSON, nullable=False)
    error_count = Column(Integer, nullable=False, default=0)


def create_database(engine, drop=False):
    if drop:
        MonitorBase.metadata.drop_all(bind=engine)
    MonitorBase.metadata.create_all(bind=engine)
