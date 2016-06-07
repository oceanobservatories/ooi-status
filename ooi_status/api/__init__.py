import os
from flask import Flask
from flask.json import JSONEncoder
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from ..model.status_model import Base


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

app.config.from_object('ooi_status.default_settings')
if 'OOISTATUS_SETTINGS' in os.environ:
    app.config.from_envvar('OOISTATUS_SETTINGS')
app.json_encoder = StatusJsonEncoder

user = app.config['USER']
password = app.config['PASSWORD']
posthost = app.config['POSTHOST']

app.engine = create_engine('postgresql+psycopg2://{user}:{password}@{posthost}'.format(user=user,
                                                                                       password=password,
                                                                                       posthost=posthost))
app.sessionmaker = sessionmaker(bind=app.engine)
app.session = scoped_session(app.sessionmaker)
Base.query = app.session.query_property()

if using_gevent:
    app.engine.pool._use_threadlocal = True

import ooi_status.api.views
