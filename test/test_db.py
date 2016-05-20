import logging
import os
import unittest

import datetime
import pandas as pd
from mock import mock
from sqlalchemy.sql.elements import and_

from ooi_status.get_logger import get_logger
from sqlalchemy import create_engine

from ooi_status.model.status_model import create_database, ExpectedStream, DeployedStream
from ooi_status.queries import get_status_by_stream_id
from ooi_status.status_monitor import CassStatusMonitor, UframeStatusMonitor
from ooi_status.stop_watch import stopwatch

log = get_logger(__name__, level=logging.INFO)

test_dir = os.path.dirname(__file__)
streamed = pd.read_csv(os.path.join(test_dir, 'data', 'ooi-status.csv'))
streamed['count'] = streamed['count'].astype('int')


def mock_query_cassandra(now, offset, rounds, offset_secs=60, num_records=100):
    fields = ['subsite', 'node', 'sensor', 'method', 'stream', 'count', 'last']
    iter = streamed[streamed.method == 'streamed'][fields].itertuples(index=False)
    collected = now - datetime.timedelta(seconds=(rounds-offset-1)*offset_secs)
    last_seen = collected - datetime.timedelta(seconds=1)
    for index, (subsite, node, sensor, method, stream, count, last) in enumerate(iter):
        if index == num_records:
            break
        reference_designator = '-'.join((subsite, node, sensor))
        particle_count = count + offset_secs * offset
        yield reference_designator, stream, method, particle_count, last_seen, collected


class StatusMonitorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine('postgresql+psycopg2://monitor@localhost/monitor_test')
        create_database(cls.engine, drop=True)


class CassStatusMonitorTest(StatusMonitorTest):
    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine('postgresql+psycopg2://monitor@localhost/monitor_test')
        create_database(cls.engine, drop=True)
        cls.monitor = CassStatusMonitor(cls.engine, None)
        cls.monitor.read_expected_csv(os.path.join(test_dir, 'data', 'expected-rates.csv'))

        rounds = 6
        with stopwatch('%d rounds' % rounds):
            now = datetime.datetime.utcnow()
            for i in xrange(rounds):
                for args in mock_query_cassandra(now, i, rounds):
                    cls.monitor._create_count(*args)

    def resolve_deployed_stream(self, name, method):
        return self.monitor.session.query(DeployedStream) \
            .join(ExpectedStream).filter(and_(ExpectedStream.name == name,
                                              ExpectedStream.method == method)).first()

    def test_read_expected(self):
        """ test read from file - depends on test_create_many_counts running first """
        with self.monitor.session.begin():
            self.assertEqual(self.monitor.session.query(ExpectedStream).count(), 373)

    def test_degraded_stream(self):
        # BOTPT rate is 20 Hz so it should be partial for our test (which updates at 1 Hz)
        ds = self.resolve_deployed_stream('botpt_nano_sample', 'streamed')
        self.assertIsNotNone(ds)
        print ds
        status = get_status_by_stream_id(self.monitor.session, ds.id, include_rates=False)
        self.assertEqual('DEGRADED', status['deployed']['status'])

    def test_operational_stream(self):
        # FLORT rate is 0.89 Hz so it should be operational for our test (which updates at 1 Hz)
        ds = self.resolve_deployed_stream('flort_d_data_record', 'streamed')
        self.assertIsNotNone(ds)
        status = get_status_by_stream_id(self.monitor.session, ds.id, include_rates=False)
        self.assertEqual('OPERATIONAL', status['deployed']['status'])


class UframeStatusMonitorTest(StatusMonitorTest):
    @classmethod
    def setUpClass(cls):
        StatusMonitorTest.setUpClass()
        cls.monitor = UframeStatusMonitor(cls.engine, 'uft21.ooi.rutgers.edu')
        cls.monitor.read_expected_csv(os.path.join(test_dir, 'data', 'expected-rates.csv'))
        cls.monitor.gather_all()

    def test_none(self):
        pass
