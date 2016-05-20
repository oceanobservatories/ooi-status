#!/usr/bin/env python

import click
from apscheduler.schedulers.blocking import BlockingScheduler
from cassandra.cluster import Cluster
from sqlalchemy import create_engine

from ooi_status.amqp_client import AmqpStatsClient
from ooi_status.get_logger import get_logger
from ooi_status.model.status_model import create_database
from ooi_status.status_monitor import CassStatusMonitor, UframeStatusMonitor

log = get_logger(__name__)


@click.command()
@click.option('--posthost', default='localhost', help='hostname for Postgres database')
@click.option('--casshost', help='hostname for the cassandra database')
@click.option('--uframehost', help='hostname for the uframe API')
def main(casshost, posthost, uframehost):
    engine = create_engine('postgresql+psycopg2://monitor:monitor@{posthost}'.format(posthost=posthost))
    create_database(engine)

    if not any((casshost, uframehost)):
        raise Exception('You must supply either a cassandra node or a uframe host')

    if all((casshost, uframehost)):
        raise Exception('You must supply only ONE cassandra node or uframe host')

    if casshost is not None:
        cluster = Cluster([casshost])
        cassandra = cluster.connect('ooi')
        monitor = CassStatusMonitor(engine, cassandra)
    else:
        monitor = UframeStatusMonitor(engine, uframehost)

    amqp_url = 'amqp://localhost'
    amqp_queue = 'port_agent_stats'
    # start the asynchronous AMQP listener
    amqp_client = AmqpStatsClient(amqp_url, amqp_queue, engine)
    amqp_client.start_thread()

    scheduler = BlockingScheduler()
    log.info('adding jobs')
    # gather data every minute
    scheduler.add_job(monitor.gather_all, 'cron', second=0)
    # resample data every hour
    scheduler.add_job(monitor.resample_count_data_hourly, 'cron', minute=0)
    # notify on change every 5 minutes
    scheduler.add_job(monitor.check_for_notify, 'cron', minute='*/5')
    log.info('starting jobs')
    scheduler.start()


if __name__ == '__main__':
    main()
