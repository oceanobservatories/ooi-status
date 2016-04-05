import logging
import os
import unittest

import pandas as pd
from get_logger import get_logger
from sqlalchemy import create_engine

from ooi_status.model import create_database, ExpectedStream, DeployedStream
from ooi_status.status_monitor import CassStatusMonitor, UframeStatusMonitor
from ooi_status.stop_watch import stopwatch

log = get_logger(__name__, level=logging.DEBUG)

test_dir = os.path.dirname(__file__)
streamed = pd.read_csv(os.path.join(test_dir, 'data', 'ooi-status.csv'))
streamed['count'] = streamed['count'].astype('int')


def mock_query_cassandra(_):
    fields = ['subsite', 'node', 'sensor', 'method', 'stream', 'count', 'last']
    out = list(streamed[fields].itertuples(index=False))
    mask = streamed['method'] == 'streamed'
    streamed['count'].values[mask.values] += 10
    streamed['last'].values[mask.values] += 10
    return out[:1000]


class StatusMonitorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine('postgresql+psycopg2://monitor@localhost/monitor_test')
        create_database(cls.engine, drop=True)


class CassStatusMonitorTest(StatusMonitorTest):
    def setUp(self):
        self.monitor = CassStatusMonitor(self.engine, None)

    def test_create_once(self):
        self.monitor._counts_from_rows(mock_query_cassandra(None))

    def test_create_many_counts(self):
        self.monitor._counts_from_rows(mock_query_cassandra(None))
        with stopwatch('10 rounds'):
            for _ in xrange(1):
                self.monitor._counts_from_rows(mock_query_cassandra(None))

    def resolve_deployed_stream(self, name):
        es = self.monitor.session.query(ExpectedStream).filter(ExpectedStream.name == name).first()
        if es is None:
            return None
        return self.monitor.session.query(DeployedStream).filter(DeployedStream.expected_stream == es).first()

    def test_read_expected(self):
        """ test read from file - depends on test_create_many_counts running first """
        self.monitor.read_expected_csv(os.path.join(test_dir, 'data', 'expected-rates.csv'))
        with self.monitor.session.begin():
            self.assertEqual(self.monitor.session.query(ExpectedStream).count(), 380)

        # populate two data points
        self.monitor._counts_from_rows(mock_query_cassandra(None))
        self.monitor._counts_from_rows(mock_query_cassandra(None))

        # telemetered streams are not getting updated, so they will be partial (if tracked)
        ds = self.resolve_deployed_stream('mopak_o_dcl_accel')
        self.assertIsNotNone(ds)
        self.assertEqual('PARTIAL', self.monitor.status(ds))

        ds = self.resolve_deployed_stream('cg_dcl_eng_dcl_gps_recovered')
        self.assertIsNotNone(ds)
        self.assertEqual('OPERATIONAL', self.monitor.status(ds))

        # CTDBP rate is 1.8 Hz so it should be partial for our test (which updates at 1 Hz)
        ds = self.resolve_deployed_stream('ctdbp_no_sample')
        self.assertIsNotNone(ds)
        self.assertEqual('PARTIAL', self.monitor.status(ds))

        # ADCP rate is 0.96 Hz so it should be operational for our test (which updates at 1 Hz)
        ds = self.resolve_deployed_stream('adcp_velocity_beam')
        self.assertIsNotNone(ds)
        self.assertEqual('OPERATIONAL', self.monitor.status(ds))


class UframeStatusMonitorTest(StatusMonitorTest):
    def setUp(self):
        self.monitor = UframeStatusMonitor(self.engine, 'uft21.ooi.rutgers.edu')
        self.refdes = 'RS10ENGC-XX00X-00-PRESTA001'
        self.stream = 'prest_real_time'
        self.method = 'streamed'
        with self.monitor.session.begin():
            self.ds = self.monitor._get_or_create_stream(self.refdes, self.stream, self.method)

    def test_query_api(self):
        count, timestamp = self.monitor._query_api(self.ds)
        self.assertNotEqual(count, 0)
        self.assertNotEqual(timestamp, 0)

    def test_create_counts(self):
        counts_obj = self.monitor._create_counts(self.ds)
        self.assertEqual(self.refdes, counts_obj.stream.ref_des.name)
        self.assertEqual(self.stream, counts_obj.stream.expected_stream.name)
        self.assertEqual(self.method, counts_obj.stream.expected_stream.method)
        self.assertNotEqual(counts_obj.particle_count, 0)
        self.assertNotEqual(counts_obj.timestamp, 0)
