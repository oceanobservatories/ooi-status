"""
Track the CI particles being ingested into cassandra and store information into Postgres.

@author Dan Mergens
"""
from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class ReferenceDesignator(Base):
    __tablename__ = 'reference_designator'
    __table_args__ = (
        UniqueConstraint('name'),
    )
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

    def __repr__(self):
        return self.name


class ExpectedStream(Base):
    __tablename__ = 'expected_stream'
    __table_args__ = (
        UniqueConstraint('name', 'method'),
    )
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    method = Column(String, nullable=False)
    expected_rate = Column(Float)
    warn_interval = Column(Integer)
    fail_interval = Column(Integer)

    def asdict(self):
        fields = ['id', 'name', 'method', 'expected_rate', 'warn_interval', 'fail_interval']
        return {field: getattr(self, field) for field in fields}

    def __repr__(self):
        return '{0} {1} {2} Hz {3}/{4}'.format(self.name, self.method, self.expected_rate,
                                               self.warn_interval, self.fail_interval)


class DeployedStream(Base):
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
    expected_rate = Column(Float)
    warn_interval = Column(Integer)
    fail_interval = Column(Integer)
    reference_designator = relationship(ReferenceDesignator, backref='deployed_streams', lazy='joined')
    expected_stream = relationship(ExpectedStream, backref='deployed_streams', lazy='joined')
    stream_condition = relationship('StreamCondition', uselist=False, back_populates="deployed_stream")

    def asdict(self):
        return {
            'id': self.id,
            'reference_designator': self.reference_designator.name,
            'expected_stream': self.expected_stream,
            'expected_rate': self.expected_rate,
            'warn_interval': self.warn_interval,
            'fail_interval': self.fail_interval,
        }

    def __repr__(self):
        return '{0} {1} {2} {3}'.format(self.reference_designator, self.expected_stream,
                                        self.collected, self.particle_count)

    def get_expected_rate(self):
        return self.expected_stream.expected_rate if self.expected_rate is None else self.expected_rate

    def get_warn_interval(self):
        return self.expected_stream.warn_interval if self.warn_interval is None else self.warn_interval

    def get_fail_interval(self):
        return self.expected_stream.fail_interval if self.fail_interval is None else self.fail_interval

    @property
    def disabled(self):
        return all((
            self.expected_rate == 0,
            self.warn_interval == 0,
            self.fail_interval == 0
        ))

    def disable(self):
        self.expected_rate = 0
        self.warn_interval = 0
        self.fail_interval = 0

    def enable(self):
        self.expected_rate = None
        self.warn_interval = None
        self.fail_interval = None


class StreamCondition(Base):
    __tablename__ = 'stream_condition'
    __table_args__ = (
        UniqueConstraint('stream_id'),
    )
    id = Column(Integer, primary_key=True)
    stream_id = Column(Integer, ForeignKey('deployed_stream.id'), nullable=False)
    last_status_time = Column(DateTime, nullable=False)
    last_status = Column(String, nullable=False)

    deployed_stream = relationship('DeployedStream', back_populates='stream_condition')


class StreamCount(Base):
    __tablename__ = 'stream_count'
    id = Column(Integer, primary_key=True)
    stream_id = Column(Integer, ForeignKey('deployed_stream.id'), nullable=False)
    collected_time = Column(DateTime, nullable=False)
    particle_count = Column(Integer, default=0)
    seconds = Column(Float, default=0)
    stream = relationship(DeployedStream, backref='stream_counts')

    def __repr__(self):
        return 'StreamCount(id=%d, count=%d, seconds=%f, rate=%f)' % (self.id,
                                                                      self.particle_count,
                                                                      self.seconds,
                                                                      self.particle_count / self.seconds)


class PortCount(Base):
    __tablename__ = 'port_count'
    id = Column(Integer, primary_key=True)
    reference_designator_id = Column(Integer, ForeignKey('reference_designator.id'), nullable=False)
    collected_time = Column(DateTime, nullable=False)
    byte_count = Column(Integer, default=0)
    seconds = Column(Float, default=0)
    reference_designator = relationship(ReferenceDesignator, backref='port_counts')


def create_database(engine, drop=False):
    if drop:
        Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
