"""
RSN health and status monitor for data particles received by OOI CI.

@author Dan Mergens
"""
import datetime
import logging

import pandas as pd
import requests
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.elements import and_

from get_logger import get_logger
from ooi_status.model.status_model import Counts, DeployedStream, ReferenceDesignator, ExpectedStream
from stop_watch import stopwatch

log = get_logger(__name__, logging.DEBUG)


class BaseStatusMonitor(object):
    ntp_epoch_offset = (datetime.datetime(1970, 1, 1) - datetime.datetime(1900, 1, 1)).total_seconds()

    def __init__(self, engine):
        self.engine = engine
        session_factory = sessionmaker(bind=engine, autocommit=True)
        self.session = session_factory()
        self._last_count = {}
        self._refdes_cache = {}
        self._expected_cache = {}
        self._deployed_cache = {}

    def gather_all(self):
        raise NotImplemented

    def status(self, deployed_stream):
        state = 'FAILURE'
        rate = self.last_rate(deployed_stream)
        warn_interval = deployed_stream.expected_stream.warn_interval
        fail_interval = deployed_stream.expected_stream.fail_interval
        expected_rate = deployed_stream.expected_stream.rate

        log.debug('rate (cur/exp): %s/%s ', rate, expected_rate)
        if not fail_interval or warn_interval and rate < warn_interval:
            state = 'OPERATIONAL'
            if expected_rate and rate < expected_rate:
                state = 'PARTIAL'
        elif fail_interval and rate < fail_interval:
            state = 'PARTIAL'

        return state

    def last_rate(self, deployed_stream):
        with self.session.begin():
            counts = self.session.query(Counts).filter(
                Counts.stream == deployed_stream).order_by(Counts.timestamp.desc())[:2]
            log.debug('counts: %s', counts)
            if len(counts) < 2:
                return 0
            return counts[0].rate(counts[1])

    def _get_or_create_refdes(self, name):
        if name not in self._refdes_cache:
            refdes = self.session.query(ReferenceDesignator).filter(ReferenceDesignator.name == name).first()
            if refdes is None:
                refdes = ReferenceDesignator(name=name)
                self.session.add(refdes)
                self.session.flush()
            self._refdes_cache[name] = refdes
        return self._refdes_cache[name]

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

    def _get_or_create_stream(self, refdes, stream, method):
        refdes_obj = self._get_or_create_refdes(refdes)
        expected_obj = self._get_or_create_expected(stream, method)
        if (refdes_obj, expected_obj) not in self._deployed_cache:
            deployed = self.session.query(DeployedStream).filter(
                and_(DeployedStream.ref_des == refdes_obj, DeployedStream.expected_stream == expected_obj)).first()
            if deployed is None:
                deployed = DeployedStream(ref_des=refdes_obj, expected_stream=expected_obj)
                self.session.add(deployed)
                self.session.flush()
            self._deployed_cache[(refdes_obj, expected_obj)] = deployed
        return self._deployed_cache[(refdes_obj, expected_obj)]

    def _get_last_count(self, refdes, stream, method):
        key = (refdes, stream, method)
        deployed_stream = None
        if key not in self._last_count:
            deployed_stream = self._get_or_create_stream(refdes, stream, method)
            last = self.session.query(Counts) \
                .filter(Counts.stream == deployed_stream) \
                .order_by(Counts.timestamp.desc()).first()
            if last:
                self._last_count[key] = last
        return self._last_count.get(key), deployed_stream

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
                es.rate = rate
                es.warn_interval = timeout * 2
                es.fail_interval = timeout * 10
                self.session.add(es)


class CassStatusMonitor(BaseStatusMonitor):
    def __init__(self, engine, cassandra_session):
        super(CassStatusMonitor, self).__init__(engine)
        self.cassandra = cassandra_session

    def gather_all(self):
        self._counts_from_rows(self._query_cassandra())

    @stopwatch()
    def _counts_from_rows(self, rows):
        with self.session.begin():
            added = 0
            for index, (subsite, node, sensor, method, stream, count, ntp_timestamp) in enumerate(rows):
                timestamp = datetime.datetime.utcfromtimestamp(ntp_timestamp - self.ntp_epoch_offset)
                refdes = '-'.join((subsite, node, sensor))
                last_count, deployed = self._get_last_count(refdes, stream, method)

                if not last_count or (last_count and last_count.particle_count != count):
                    added += 1
                    if deployed is None:
                        deployed = self._get_or_create_stream(refdes, stream, method)
                    count = Counts(stream=deployed, particle_count=count, timestamp=timestamp)
                    self.session.add(count)
                    self._last_count[deployed] = count
            log.debug('ADDED: %d', added)

    def _query_cassandra(self):
        return self.cassandra.execute('select subsite, node, sensor, method, stream, count, last from stream_metadata')


class UframeStatusMonitor(BaseStatusMonitor):
    EDEX_BASE_URL = 'http://%s:%d/sensor/inv/%s/%s/%s'

    def __init__(self, engine, uframe_host, uframe_port=12576):
        super(UframeStatusMonitor, self).__init__(engine)
        self.uframe_host = uframe_host
        self.uframe_port = uframe_port

    @stopwatch('gather_all:')
    def gather_all(self):
        with self.session.begin():
            for stream in self.session.query(DeployedStream).all():
                log.info('stream is: %s', stream)
                self._create_counts(stream)

    def _create_counts(self, deployed_stream):
        count, timestamp = self._query_api(deployed_stream)
        counts = Counts(stream=deployed_stream, particle_count=count, timestamp=timestamp)
        self.session.add(counts)
        return counts

    def _query_api(self, deployed_stream):
        """
        Get most recent metadata for the stream from cassandra
        :param deployed_stream: deployed stream object from postgres
        :return: (count, timestamp)
        """
        count = 0  # total number of particles for this stream in cassandra
        timestamp = 0  # last timestamp for this stream in cassandra

        subsite, node, sensor = self.parse_reference_designator(deployed_stream.ref_des.name)
        stream = deployed_stream.expected_stream.name
        method = deployed_stream.expected_stream.method

        # get the latest metadata from cassandra
        url = self.EDEX_BASE_URL % (self.uframe_host, self.uframe_port, subsite, node, sensor) + '/metadata/times'
        response = requests.get(url)
        if response.status_code is not 200:
            log.error('failed to get a valid JSON response')
            return count, timestamp

        # find the matching stream name and method in the return
        for stream_dict in response.json():
            if stream_dict['stream'] == stream and stream_dict['method'] == method:
                count = stream_dict['count']
                timestamp = stream_dict['endTime']
                break

        return count, timestamp

    @staticmethod
    def parse_reference_designator(ref_des):
        subsite, node, sensor = ref_des.split('-', 2)
        return subsite, node, sensor
