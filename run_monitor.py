#!/usr/bin/env python

import click
from apscheduler.schedulers.blocking import BlockingScheduler
from cassandra.cluster import Cluster
from sqlalchemy import create_engine

from ooi_status.get_logger import get_logger
from ooi_status.model.status_model import create_database
from ooi_status.status_monitor import CassStatusMonitor


log = get_logger(__name__)


@click.command()
@click.option('--posthost', default='localhost', help='hostname for Postgres database')
@click.option('--casshost', default='localhost', help='hostname for the cassandra database')
def main(casshost, posthost):
    engine = create_engine('postgresql+psycopg2://monitor:monitor@{posthost}'.format(posthost=posthost))
    create_database(engine)

    if casshost is not None:
        cluster = Cluster([casshost])
        cassandra = cluster.connect('ooi')
    else:
        cassandra = None

    monitor = CassStatusMonitor(engine, cassandra)
    scheduler = BlockingScheduler()
    log.info('adding job')
    scheduler.add_job(monitor.gather_all, 'cron', second=0)
    # scheduler.add_job(monitor.gather_all, 'interval', seconds=60)
    log.info('starting job')
    scheduler.start()


if __name__ == '__main__':
    main()
