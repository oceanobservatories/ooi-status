"""
RSN health and status monitor for data particles received by OOI CI.

@author Dan Mergens
"""
import datetime
import logging
import os

import click
import pandas as pd
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from cachetools import LRUCache, cached
from flask import Config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import ObjectDeletedError

from ooi_data.postgres.model import DeployedStream, ExpectedStream, ReferenceDesignator, PendingUpdate, StatusEnum

from ooi_status.event_notifier import EventNotifier
from ooi_status.metadata_queries import get_active_streams
from ooi_status.status_message import StatusMessage
from .get_logger import get_logger
from .queries import (resample_port_count, get_port_rates_dataframe, get_rollup_status)
from .stop_watch import stopwatch

log = get_logger(__name__, logging.INFO)
here = os.path.dirname(__file__)

MAX_STATUS_POST_FAILURES = 5
STREAM_CACHE = LRUCache(3000)


class StatusMonitor(object):
    def __init__(self, config):
        self.config = config

        self.engine = create_engine(config['MONITOR_URL'])
        self.metadata_engine = create_engine(config['METADATA_URL'])

        self.session_factory = sessionmaker(bind=self.engine, autocommit=True)
        self.session = self.session_factory()

        self.metadata_session_factory = sessionmaker(bind=self.metadata_engine, autocommit=True)
        self.metadata_session = self.metadata_session_factory()

    @cached(STREAM_CACHE)
    def _get_or_create_stream(self, refdes, stream, method):
        refdes_obj = ReferenceDesignator.get_or_create(self.session, refdes)
        expected_obj = ExpectedStream.get_or_create(self.session, stream, method)
        return DeployedStream.get_or_create(self.session, refdes_obj, expected_obj)

    def get_or_create_stream(self, refdes, stream, method):
        """
        Attempt to fetch this value from the cached method. If the cached value no longer exists,
        delete from the cache and create a new object.
        """
        try:
            ds = self._get_or_create_stream(refdes, stream, method)
            s = ds.status
        except ObjectDeletedError:
            del STREAM_CACHE[(self, refdes, stream, method)]
            ds = self._get_or_create_stream(refdes, stream, method)
        return ds

    @stopwatch()
    def read_expected_csv(self, filename):
        """ Populate expected stream definitions from definition in CSV-formatted file"""
        log.info('Populating the expected streams table')
        df = pd.read_csv(filename)
        fields = ['name', 'method', 'expected_rate', 'warn_interval', 'fail_interval']
        with self.session.begin():
            for stream, method, rate, warn_interval, fail_interval in df[fields].itertuples(index=False):
                es = ExpectedStream.get_or_create(self.session, stream, method)
                es.expected_rate = rate
                es.warn_interval = warn_interval
                es.fail_interval = fail_interval
                self.session.add(es)

    @stopwatch()
    def _check_status(self, rows):
        now = datetime.datetime.utcnow()

        messages = []
        with self.session.begin():

            for stream_metadata, elapsed, uid in rows:
                deployed = self.get_or_create_stream(stream_metadata.refdes,
                                                     stream_metadata.stream,
                                                     stream_metadata.method)

                status, interval = deployed.get_status(elapsed)

                if deployed.status == status:
                    continue

                messages.append(StatusMessage(stream_metadata.refdes,
                                              stream_metadata.stream,
                                              uid,
                                              elapsed,
                                              deployed.status,
                                              status,
                                              interval))
                deployed.status = status
                deployed.status_time = now

        return messages

    @stopwatch()
    def _add_rollup_status(self, in_messages):
        status_dict = {}
        out_messages = []
        with self.session.begin():
            for each in in_messages:
                rollup_status, rollup_reason = status_dict.get(each.refdes, (None, None))
                if rollup_status is None:
                    rollup_status, rollup_reason = get_rollup_status(self.session, each.refdes)
                    status_dict[each.refdes] = rollup_status, rollup_reason
                each.instrument_status = rollup_status
                each.instrument_reason = rollup_reason
                out_messages.append(each)
        return out_messages

    def resample_count_data_hourly(self):
        window_start = self.config.get('RESAMPLE_WINDOW_START_HOURS')
        window_end = self.config.get('RESAMPLE_WINDOW_END_HOURS')
        # get a datetime object representing this HOUR
        now = datetime.datetime.utcnow().replace(second=0, minute=0)
        window_start_dt = now - datetime.timedelta(hours=window_start)
        window_end_dt = now - datetime.timedelta(hours=window_end)
        session = self.session_factory()

        for reference_designator in self.session.query(ReferenceDesignator):
            log.info('Resampling %s', reference_designator)
            with session.begin():
                counts_df = get_port_rates_dataframe(session, reference_designator.id, window_end_dt, window_start_dt)
                resample_port_count(session, reference_designator.id, counts_df, 3600)

    def get_status_notifier(self):
        root_url = self.config.get('NOTIFY_URL_ROOT')
        event_port = self.config.get('NOTIFY_URL_PORT')
        return EventNotifier(self.session, root_url, event_port)

    @stopwatch()
    def save_pending(self, messages):
        with self.session.begin():
            for message in messages:
                log.info('Staging status message: %r', message)
                self.session.add(PendingUpdate(message=message.as_dict()))

    def check_all(self):
        active = get_active_streams(self.metadata_session)
        changed = self._check_status(active)
        rolled = self._add_rollup_status(changed)
        self.save_pending(rolled)

    def notify_all(self):
        notifier = self.get_status_notifier()
        session = self.session_factory()

        with session.begin():
            for pu in session.query(PendingUpdate).order_by(PendingUpdate.id):
                message = pu.message
                uid = message.get('assetUid')
                delete = False
                if uid and message:
                    try:
                        response = notifier.post_event(uid, message)
                        response.raise_for_status()
                        status_code = response.status_code
                        if status_code == 201:
                            delete = True
                        elif 400 <= status_code < 500:
                            # client error - increment the error count
                            log.error('Received client error from events API: (%d) %r',
                                      status_code, response.content)
                            pu.error_count += 1
                            if pu.error_count > MAX_STATUS_POST_FAILURES:
                                delete = True
                        elif status_code >= 500:
                            # server error - don't increment error count
                            # log the problem
                            log.error('Received server error from events API: (%d) %r',
                                      status_code, response.content)
                            continue
                        else:
                            # unknown response
                            # log, but don't increment error count
                            log.error('Received unexpected response from events API: (%d) %r',
                                      status_code, response.content)
                    except requests.exceptions.RequestException:
                        # Don't count this as an error
                        # we'll keep trying until we can connect to uframe
                        continue
                if delete:
                    session.delete(pu)


@click.command()
@click.option('--expected', type=click.Path(exists=True, dir_okay=False),
              help='CSV file with expected rates and timeouts')
def main(expected):
    config = Config(here)
    config.from_object('ooi_status.default_settings')
    if 'OOISTATUS_SETTINGS' in os.environ:
        config.from_envvar('OOISTATUS_SETTINGS')

    for key in config:
        log.info('OOI_STATUS CONFIG: %r: %r', key, config[key])

    monitor = StatusMonitor(config)

    if expected:
        monitor.read_expected_csv(expected)

    else:
        scheduler = BlockingScheduler()
        log.info('adding jobs')

        # notify on change every minute
        scheduler.add_job(monitor.check_all, 'cron', second=0)
        scheduler.add_job(monitor.notify_all, 'cron', second=10)
        log.info('starting jobs')
        scheduler.start()


if __name__ == '__main__':
    main()
