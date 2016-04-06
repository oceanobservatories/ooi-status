from datetime import datetime

import numpy as np
import pandas as pd
from flask import jsonify, request
from werkzeug.exceptions import abort

from ooi_status.api import app, db
from ooi_status.model.status_model import ExpectedStream, DeployedStream, ReferenceDesignator, Counts
from ooi_status.queries import get_status_query, get_hourly_rates


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
    for deployed_stream, last_seen_time, current_count, five_mins, one_day in query:
        if deployed_stream.ref_des.name.startswith('RS10'):
            continue

        row_dict = create_status_dict(deployed_stream, base_time, last_seen_time, current_count, five_mins, one_day)
        if not filter_status or filter_status == row_dict['status']:
            out.append(row_dict)

    counts = {
        'dead': len([row for row in out if row['status'] == 'DEAD']),
        'failed': len([row for row in out if row['status'] == 'FAILED']),
        'degraded': len([row for row in out if row['status'] == 'DEGRADED']),
        'ignored': len([row for row in out if row['status'] == 'NOSTATUS']),
        'operational': len([row for row in out if row['status'] == 'OPERATIONAL'])
    }
    return jsonify({'counts': counts, 'status': out, 'num_records': len(out)})


@app.route('/status/<deployed_id>')
def last_seen_detail(deployed_id):
    base_time = datetime.utcnow().replace(second=0, microsecond=0)
    query = get_status_query(db.session, base_time, stream_id=deployed_id)
    result = query.first()
    if result is not None:
        deployed_stream, last_seen_time, current_count, five_mins, one_day = result

        row_dict = create_status_dict(deployed_stream, base_time, last_seen_time, current_count, five_mins, one_day)
        counts_df = get_hourly_rates(db.session, deployed_id)
        return jsonify({'deployed': row_dict, 'hours': list(counts_df.index), 'rates': list(counts_df.rate)})

    abort(404)


def create_status_dict(deployed_stream, base_time, last_seen_time, current_count, five_mins, one_day):
    row_dict = deployed_stream.asdict()
    expected_stream = deployed_stream.expected_stream
    five_min_rate = (current_count - five_mins) / 300.0
    one_day_rate = (current_count - one_day) / 86400.0

    row_dict['current_count'] = current_count
    row_dict['five_min_count'] = five_mins
    row_dict['one_day_count'] = one_day
    row_dict['last_seen'] = last_seen_time
    row_dict['five_min_rate'] = five_min_rate
    row_dict['one_day_rate'] = one_day_rate

    elapsed = base_time - last_seen_time
    elasped_seconds = elapsed.total_seconds()

    if expected_stream.rate:
        TWENTY_PERCENT = 0.2
        five_min_percent = 1 - TWENTY_PERCENT
        one_day_percent = 1 - (TWENTY_PERCENT / (86400 / 300.0))
        five_min_thresh = expected_stream.rate * five_min_percent
        one_day_thresh = expected_stream.rate * one_day_percent
        row_dict['one_day_thresh'] = one_day_thresh
        row_dict['five_min_thresh'] = five_min_thresh

    if not any([expected_stream.rate, expected_stream.fail_interval, expected_stream.warn_interval]):
        status = 'NOSTATUS'
    elif elasped_seconds > expected_stream.fail_interval * 700:
        status = 'DEAD'
    elif elasped_seconds > expected_stream.fail_interval:
        status = 'FAILED'
    elif elasped_seconds > expected_stream.warn_interval or (five_min_rate < five_min_thresh and
                                                                     one_day_rate < one_day_thresh):
        status = 'DEGRADED'
    else:
        status = 'OPERATIONAL'
    row_dict['status'] = status
    row_dict['elapsed'] = str(elapsed)
    return row_dict

