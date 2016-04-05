from datetime import datetime

from flask import jsonify, request
from sqlalchemy import func
from sqlalchemy.sql.elements import and_

from ooi_status.api import app, db
from ooi_status.model.status_model import ExpectedStream, DeployedStream, ReferenceDesignator, Counts


@app.route('/expected')
def expected():
    expected = db.session.query(ExpectedStream).all()
    return jsonify({'expected_streams': [e.asdict() for e in expected]})


@app.route('/deployed')
def deployed():
    deployed = db.session.query(DeployedStream)[:50]
    out = []
    for each in deployed:
        each_dict = each.asdict()
        each_dict['last_seen'] = each.last_seen(db.session)
        out.append(each_dict)
    return jsonify({'deployed_streams': out})


@app.route('/reference_designator')
def refdes():
    refs = db.session.query(ReferenceDesignator).all()
    return jsonify({'reference_designators': [r.asdict() for r in refs]})


@app.route('/5mins')
def five_mins():
    out = []
    rows = db.session.execute('select * from five_minute_rate')
    for refdes, method, stream, rate, last_seen in rows:
        out.append({
            'ref_des': refdes,
            'method': method,
            'stream': stream,
            'rate': rate,
            'last_seen': last_seen
        })
    return jsonify({'5mins': out})


@app.route('/status')
def last_seen():
    filter_status = request.args.get('status')
    filter_refdes = request.args.get('refdes')
    filter_method = request.args.get('method')
    filter_stream = request.args.get('stream')
    filter_constraints = []
    if filter_refdes:
        filter_constraints.append(ReferenceDesignator.name.like('%%%s%%' % filter_refdes))
    if filter_method:
        filter_constraints.append(ExpectedStream.method.like('%%%s%%' % filter_method))
    if filter_stream:
        filter_constraints.append(ExpectedStream.name.like('%%%s%%' % filter_stream))

    subquery = db.session.query(Counts.stream_id,
                                func.max(Counts.timestamp).label('last_seen')).group_by(Counts.stream_id).subquery()

    query = db.session.query(DeployedStream,
                             subquery.c.last_seen).join(subquery).join(ExpectedStream).join(ReferenceDesignator)

    if filter_constraints:
        query = query.filter(and_(*filter_constraints))

    out = []
    for deployed_stream, last_seen in query:

        row_dict = deployed_stream.asdict()
        expected = deployed_stream.expected_stream
        row_dict['last_seen'] = last_seen

        elapsed = datetime.utcnow() - last_seen
        elasped_seconds = elapsed.total_seconds()

        if deployed_stream.ref_des.name.startswith('RS10'):
            continue

        if not any([expected.rate, expected.fail_interval, expected.warn_interval]):
            status = 'NOSTATUS'
        elif elasped_seconds > expected.fail_interval * 700:
            status = 'DEAD'
        elif elasped_seconds > expected.fail_interval:
            status = 'FAILED'
        elif elasped_seconds > expected.warn_interval:
            status = 'DEGRADED'
        else:
            status = 'OPERATIONAL'
        row_dict['status'] = status
        row_dict['elapsed'] = str(elapsed)

        if not filter_status or filter_status == status:
            out.append(row_dict)

    return jsonify({'last_seen': out, 'query': str(query).split('\n')})

