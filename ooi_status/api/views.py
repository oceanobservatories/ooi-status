from collections import Counter
from datetime import datetime

import numpy as np
import pandas as pd
from flask import jsonify, request
from werkzeug.exceptions import abort

from ooi_status.api import app, db
from ooi_status.model.status_model import ExpectedStream, DeployedStream, ReferenceDesignator, Counts
from ooi_status.queries import get_status_query, get_hourly_rates, create_status_dict


@app.route('/expected')
def expected():
    expected = db.session.query(ExpectedStream).all()
    return jsonify({'expected_streams': [e.asdict() for e in expected]})


@app.route('/deployed/<deployed_id>')
def expected_detail(deployed_id):
    deployed = db.session.query(DeployedStream).get(deployed_id)
    query = db.session.query(Counts).filter(Counts.stream == deployed)
    counts_df = pd.read_sql_query(query.statement, query.session.bind)
    counts_df['particle_count'] = counts_df.particle_count.diff()
    counts_df['elapsed'] = counts_df.timestamp.diff() / np.timedelta64(1, 's')
    counts_df['rate'] = counts_df.particle_count / counts_df.elapsed
    counts_df = counts_df.set_index('timestamp')
    counts_df = counts_df[['rate']].resample('1H').mean()
    return jsonify({'deployed': deployed, 'hours': list(counts_df.index), 'rates': list(counts_df.rate)})


@app.route('/reference_designator')
def refdes():
    refs = db.session.query(ReferenceDesignator).all()
    return jsonify({'reference_designators': [r.asdict() for r in refs]})


@app.route('/status')
def last_seen():
    filter_status = request.args.get('status')
    filter_refdes = request.args.get('refdes')
    filter_method = request.args.get('method')
    filter_stream = request.args.get('stream')

    base_time = datetime.utcnow().replace(second=0, microsecond=0)
    query = get_status_query(db.session, base_time, filter_refdes, filter_method, filter_stream)

    out = []
    for row in query:
        row_dict = create_status_dict(row, base_time)
        # FILTER BENCH INSTRUMENTS
        if row_dict['ref_des'].name.startswith('RS10'):
            continue

        if not filter_status or filter_status == row_dict['status']:
            out.append(row_dict)

    counts = Counter()
    for row in out:
        counts[row['status']] += 1

    # FILTER UNTRACKED STREAMS
    out = [row for row in out if row['status'] != 'NOSTATUS']

    return jsonify({'counts': counts, 'status': out, 'num_records': len(out)})


@app.route('/status/<deployed_id>')
def last_seen_detail(deployed_id):
    base_time = datetime.utcnow().replace(second=0, microsecond=0)
    query = get_status_query(db.session, base_time, stream_id=deployed_id)
    result = query.first()
    if result is not None:
        row_dict = create_status_dict(result, base_time)
        counts_df = get_hourly_rates(db.session, deployed_id)
        return jsonify({'deployed': row_dict, 'hours': list(counts_df.index), 'rates': list(counts_df.rate)})

    abort(404)


