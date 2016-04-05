#!/usr/bin/env python

import click
from sqlalchemy import create_engine

from ooi_status.get_logger import get_logger
from ooi_status.model.status_model import create_database
from ooi_status.status_monitor import CassStatusMonitor

log = get_logger(__name__)


@click.command()
@click.option('--posthost', default='localhost', help='hostname for Postgres database')
@click.option('--expected', type=click.Path(exists=True, dir_okay=False),
              help='CSV file with expected rates and timeouts')
def main(posthost, expected):
    engine = create_engine('postgresql+psycopg2://monitor:monitor@{posthost}'.format(posthost=posthost))
    create_database(engine)
    monitor = CassStatusMonitor(engine, None)
    monitor.read_expected_csv(expected)


if __name__ == '__main__':
    main()
