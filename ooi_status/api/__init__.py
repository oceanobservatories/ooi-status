import os
from flask import Flask
from flask.json import JSONEncoder
from flask_sqlalchemy import SQLAlchemy


class StatusJsonEncoder(JSONEncoder):
    def default(self, o):
        if hasattr(o, 'asdict'):
            return o.asdict()
        return JSONEncoder.default(self, o)


if 'PSYCOGREEN' in os.environ:
    from gevent.monkey import patch_all
    patch_all()
    from psycogreen.gevent import patch_psycopg
    patch_psycopg()

    using_gevent = True
else:
    using_gevent = False


app = Flask(__name__)
app.json_encoder = StatusJsonEncoder
app.config['SQLALCHEMY_ECHO'] = False
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql+psycopg2://monitor:monitor@localhost'
db = SQLAlchemy(app)

if using_gevent:
    db.engine.pool._use_threadlocal = True

import ooi_status.api.views
