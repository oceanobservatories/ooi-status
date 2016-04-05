#!/usr/bin/env python

import click
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func
from sqlalchemy.sql.elements import and_
from IPython import embed

from ooi_status.get_logger import get_logger
from ooi_status.model.status_model import *

log = get_logger(__name__)


@click.command()
@click.option('--posthost', default='localhost', help='hostname for Postgres database')
def main(posthost):
    engine = create_engine('postgresql+psycopg2://monitor:monitor@{posthost}'.format(posthost=posthost))
    create_database(engine)
    session = sessionmaker(bind=engine, autocommit=True)()
    embed()


if __name__ == '__main__':
    main()
