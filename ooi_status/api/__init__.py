from flask import Flask
from flask.json import JSONEncoder
from flask_sqlalchemy import SQLAlchemy


class StatusJsonEncoder(JSONEncoder):
    def default(self, o):
        if hasattr(o, 'asdict'):
            return o.asdict()
        return JSONEncoder.default(self, o)

app = Flask(__name__)
app.json_encoder = StatusJsonEncoder
app.config['SQLALCHEMY_ECHO'] = True
db = SQLAlchemy(app)

import ooi_status.api.views
