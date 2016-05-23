"""
RSN health and status monitor for data particles received by OOI CI.

@author Dan Mergens
"""
import datetime
import logging
import pandas as pd
import requests

from dateutil.parser import parse
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.elements import and_

from .get_logger import get_logger
from .model.status_model import DeployedStream, ExpectedStream, StreamCount, ReferenceDesignator
from .queries import resample_stream_count, get_status_for_notification, resample_port_count
from .stop_watch import stopwatch

log = get_logger(__name__, logging.INFO)


class BaseStatusMonitor(object):
    def __init__(self, engine):
        self.engine = engine
        self.session_factory = sessionmaker(bind=engine, autocommit=True)
        self.session = self.session_factory()
        self._last_count = {}
        self._refdes_cache = {}
        self._expected_cache = {}
        self._deployed_cache = {}

    def gather_all(self):
        raise NotImplemented

    def _get_or_create_refdes(self, reference_designator):
        if reference_designator not in self._refdes_cache:
            refdes = self.session.query(ReferenceDesignator).filter(
                ReferenceDesignator.name == reference_designator).first()
            if refdes is None:
                refdes = ReferenceDesignator(name=reference_designator)
                self.session.add(refdes)
                self.session.flush()
            self._refdes_cache[reference_designator] = refdes
        return self._refdes_cache[reference_designator]

    def _get_or_create_expected(self, stream, method):
        if (stream, method) not in self._expected_cache:
            expected = self.session.query(ExpectedStream).filter(
                and_(ExpectedStream.name == stream, ExpectedStream.method == method)).first()
            if expected is None:
                expected = ExpectedStream(name=stream, method=method)
                self.session.add(expected)
                self.session.flush()
            self._expected_cache[(stream, method)] = expected
        return self._expected_cache[(stream, method)]

    def _get_or_create_stream(self, refdes, stream, method, count, timestamp, coll_time):
        refdes_obj = self._get_or_create_refdes(refdes)
        expected_obj = self._get_or_create_expected(stream, method)
        if (refdes, expected_obj) not in self._deployed_cache:
            deployed = self.session.query(DeployedStream).filter(
                and_(DeployedStream.reference_designator == refdes_obj,
                     DeployedStream.expected_stream == expected_obj)).first()
            if deployed is None:
                deployed = DeployedStream(reference_designator=refdes_obj, expected_stream=expected_obj,
                                          particle_count=count, last_seen=timestamp, collected=coll_time)
                self.session.add(deployed)
                self.session.flush()
            self._deployed_cache[(refdes, expected_obj)] = deployed
        return self._deployed_cache[(refdes, expected_obj)]

    @stopwatch()
    def read_expected_csv(self, filename):
        """ Populate expected stream definitions from definition in CSV-formatted file"""
        df = pd.read_csv(filename)
        fields = ['stream', 'method', 'expected rate (Hz)', 'timeout']
        with self.session.begin():
            for stream, method, rate, timeout in df[fields].itertuples(index=False):
                es = self.session.query(ExpectedStream).filter(and_(ExpectedStream.name == stream,
                                                                    ExpectedStream.method == method)).first()
                if es is None:
                    es = ExpectedStream(name=stream, method=method)
                es.expected_rate = rate
                es.warn_interval = timeout * 2
                es.fail_interval = timeout * 10
                self.session.add(es)

    @stopwatch()
    def _create_counts(self, rows):
        now = datetime.datetime.utcnow()
        with self.session.begin():
            for row in rows:
                log.debug('processing %s', row)
                reference_designator, method, stream, count, last_seen = row
                if method == 'streamed':
                    self._create_count(reference_designator, stream, method, count, last_seen, now)

    def _create_count(self, reference_designator, stream, method, particle_count, timestamp, now):
        deployed = self._get_or_create_stream(reference_designator, stream, method, particle_count, timestamp, now)
        if deployed.collected != now:
            count_diff = particle_count - deployed.particle_count
            time_diff = (now - deployed.collected).total_seconds()
            deployed.particle_count = particle_count
            deployed.last_seen = timestamp
            deployed.collected = now
            particle_count = StreamCount(stream_id=deployed.id, collected_time=now,
                                         particle_count=count_diff, seconds=time_diff)
            self.session.add(particle_count)

    def resample_count_data_hourly(self):
        # get a datetime object representing this HOUR
        now = datetime.datetime.utcnow().replace(second=0, minute=0)
        # get a datetime object representing this HOUR - 24
        twenty_four_ago = now - datetime.timedelta(hours=24)
        # get a datetime object representing this HOUR - 48
        fourty_eight_ago = now - datetime.timedelta(hours=48)
        session = self.session_factory()

        for deployed_stream in self.session.query(DeployedStream):
            log.info('Resampling %s', deployed_stream)
            # resample all count data from now-48 to now-24 to 1 hour
            with session.begin():
                resample_stream_count(session, deployed_stream.id, fourty_eight_ago, twenty_four_ago, 3600)

        for reference_designator in self.session.query(ReferenceDesignator):
            log.info('Resampling %s', reference_designator)
            with session.begin():
                resample_port_count(session, reference_designator.id, fourty_eight_ago, twenty_four_ago, 3600)

    def check_for_notify(self):
        session = self.session_factory()
        with session.begin():
            get_status_for_notification(session)


class CassStatusMonitor(BaseStatusMonitor):
    ntp_epoch_offset = (datetime.datetime(1970, 1, 1) - datetime.datetime(1900, 1, 1)).total_seconds()

    def __init__(self, engine, cassandra_session):
        super(CassStatusMonitor, self).__init__(engine)
        self.cassandra = cassandra_session

    def gather_all(self):
        self._create_counts(self._query_cassandra())

    def _query_cassandra(self):
        stmt = 'select subsite, node, sensor, method, stream, count, last from stream_metadata'
        for row in self.cassandra.execute(stmt):
            subsite, node, sensor, method, stream, particle_count, last_seen_ntp = row
            reference_designator = '-'.join((subsite, node, sensor))
            last_seen = datetime.datetime.utcfromtimestamp(last_seen_ntp - self.ntp_epoch_offset)
            yield reference_designator, method, stream, particle_count, last_seen


class UframeStatusMonitor(BaseStatusMonitor):
    EDEX_BASE_URL = 'http://%s:%d/sensor/inv/toc'

    def __init__(self, engine, uframe_host, uframe_port=12576):
        super(UframeStatusMonitor, self).__init__(engine)
        self.uframe_host = uframe_host
        self.uframe_port = uframe_port

    def gather_all(self):
        self._create_counts(self._query_api())

    def _query_api(self):
        """
        Get most recent metadata for the stream from cassandra
        :param deployed_stream: deployed stream object from postgres
        :return: (count, timestamp)
        """
        # get the latest metadata from cassandra
        url = self.EDEX_BASE_URL % (self.uframe_host, self.uframe_port)
        response = requests.get(url)
        if response.status_code is not 200:
            log.error('failed to get a valid JSON response')

        # find the matching stream name and method in the return
        for inst_dict in response.json():
            reference_designator = inst_dict.get('reference_designator')
            for stream_dict in inst_dict.get('streams'):
                stream = stream_dict.get('stream')
                method = stream_dict.get('method')
                particle_count = stream_dict.get('count')
                last_seen = parse(stream_dict.get('endTime')).replace(tzinfo=None)
                if all((stream, method, particle_count, last_seen)):
                    yield reference_designator, method, stream, particle_count, last_seen
