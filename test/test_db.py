import os
import datetime
import logging
import unittest
import matplotlib
import numpy as np
import pandas as pd
from sqlalchemy.sql.elements import and_
from sqlalchemy import create_engine

matplotlib.use('Agg')

from ooi_status.get_logger import get_logger
from ooi_status.model.status_model import create_database, ExpectedStream, DeployedStream
from ooi_status.queries import get_status_by_stream_id, resample_stream_count, get_stream_rates_dataframe
from ooi_status.status_monitor import BaseStatusMonitor

log = get_logger(__name__, level=logging.INFO)

test_dir = os.path.dirname(__file__)
streamed = pd.read_csv(os.path.join(test_dir, 'data', 'ooi-status.csv'))
streamed['count'] = streamed['count'].astype('int')

NTP_EPOCH = (datetime.date(1970, 1, 1) - datetime.date(1900, 1, 1)).total_seconds()


class StatusMonitorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine('postgresql+psycopg2://monitor@localhost/monitor_test')
        create_database(cls.engine, drop=True)
        cls.monitor = BaseStatusMonitor(cls.engine)
        cls.monitor.read_expected_csv(os.path.join(test_dir, 'data', 'expected-rates.csv'))

    def insert_test_data(self, refdes, stream, method, rows, rate, interval=60, end_time=None):
        if end_time is None:
            end_time = datetime.datetime.utcnow()
        counts = np.arange(0, rate*rows*interval, rate*interval)
        times = [end_time - datetime.timedelta(seconds=i * interval) for i in range(rows, 0, -1)]
        for t, c in zip(times, counts):
            self.monitor._create_count(refdes, stream, method, c, t-datetime.timedelta(seconds=1), t)
        return times[0], times[-1]

    def resolve_deployed_stream(self, name, method):
        return self.monitor.session.query(DeployedStream) \
            .join(ExpectedStream).filter(and_(ExpectedStream.name == name,
                                              ExpectedStream.method == method)).first()

    def test_read_expected(self):
        with self.monitor.session.begin():
            self.assertEqual(self.monitor.session.query(ExpectedStream).count(), 365)

    def test_degraded_stream(self):
        # BOTPT rate is 20 Hz so it should be partial for our test (which updates at 1 Hz)
        stream = 'botpt_nano_sample'
        method = 'streamed'
        self.insert_test_data('test_botpt', stream, method, 100, 1)
        ds = self.resolve_deployed_stream(stream, method)
        self.assertIsNotNone(ds)
        status = get_status_by_stream_id(self.monitor.session, ds.id, include_rates=False)
        self.assertEqual('DEGRADED', status['deployed']['status'])

    def test_operational_stream(self):
        # FLORT rate is 0.89 Hz so it should be operational for our test (which updates at 1 Hz)
        stream = 'flort_d_data_record'
        method = 'streamed'
        self.insert_test_data('test_flort', stream, method, 100, 1)
        ds = self.resolve_deployed_stream(stream, method)
        self.assertIsNotNone(ds)
        status = get_status_by_stream_id(self.monitor.session, ds.id, include_rates=False)
        self.assertEqual('OPERATIONAL', status['deployed']['status'])

    def test_resample(self):
        stream = 'test'
        method = 'test'
        # insert 2 hours worth of data, starting on the hour
        start_time = datetime.datetime(2016, 1, 1, 12, 0, 0)
        end_time = start_time + datetime.timedelta(hours=2)
        self.insert_test_data('resample_test', stream, method, 121, 1, end_time=end_time)
        # fetch our new deployed stream
        ds = self.resolve_deployed_stream(stream, method)

        # we must begin an explicit session
        with self.monitor.session.begin():
            df = get_stream_rates_dataframe(self.monitor.session, ds.id, start_time, end_time)
            resample_stream_count(self.monitor.session, ds.id, df, 3600)

        df2 = get_stream_rates_dataframe(self.monitor.session, ds.id, start_time, end_time)

        # we should have 2 resampled records, one for each hour
        self.assertEqual(df2.loc['2016-01-01 12:00:00'].particle_count, 3600)
        self.assertEqual(df2.loc['2016-01-01 13:00:00'].particle_count, 3600)
