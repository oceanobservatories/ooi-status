#!/usr/bin/env python
import click

from ooi_status.api import app


@click.command()
@click.option('--posthost', default='localhost', help='hostname for Postgres database')
def main(posthost):
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql+psycopg2://monitor:monitor@{posthost}'.format(posthost=posthost)
    app.run(host='0.0.0.0', port=12571, debug=True)


if __name__ == '__main__':
    main()
