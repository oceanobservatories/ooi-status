import os
import datetime
import logging
import unittest
import matplotlib
import numpy as np
import pandas as pd
from sqlalchemy.sql.elements import and_
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database


matplotlib.use('Agg')

from ooi_status.get_logger import get_logger
from ooi_status import model
from ooi_status.queries import get_status_by_stream_id, resample_stream_count, get_stream_rates_dataframe
from ooi_status.status_monitor import StatusMonitor

log = get_logger(__name__, level=logging.INFO)

test_dir = os.path.dirname(__file__)
streamed = pd.read_csv(os.path.join(test_dir, 'data', 'ooi-status.csv'))
streamed['count'] = streamed['count'].astype('int')

NTP_EPOCH = (datetime.date(1970, 1, 1) - datetime.date(1900, 1, 1)).total_seconds()


class StatusMonitorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine('postgresql+psycopg2://monitor@localhost/monitor_test')

        if not database_exists(cls.engine.url):
            create_database(cls.engine.url, template='template_postgis')

        model.create_database(cls.engine, drop=True)

        cls.monitor = StatusMonitor(cls.engine)
        cls.monitor.read_expected_csv(os.path.join(test_dir, 'data', 'expected-rates.csv'))

    def resolve_deployed_stream(self, name, method):
        return self.monitor.session.query(model.DeployedStream) \
            .join(model.ExpectedStream).filter(and_(model.ExpectedStream.name == name,
                                                    model.ExpectedStream.method == method)).first()

    def test_read_expected(self):
        with self.monitor.session.begin():
            self.assertEqual(self.monitor.session.query(model.ExpectedStream).count(), 365)

