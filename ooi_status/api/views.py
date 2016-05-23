import datetime
from flask import jsonify, request
from werkzeug.exceptions import abort
import six.moves.http_client as http_client

from ..api import app
from ..model.status_model import ExpectedStream, DeployedStream
from ..queries import (get_status_by_instrument, get_status_by_stream,
                       get_status_by_stream_id, resample_stream_count)


@app.teardown_appcontext
def shutdown_session(exception=None):
    app.session.remove()


@app.route('/expected')
def expected():
    filter_method = request.args.get('method')
    filter_stream = request.args.get('stream')
    expected_streams = app.session.query(ExpectedStream)

    if filter_method:
        expected_streams = expected_streams.filter(ExpectedStream.method == filter_method)

    if filter_stream:
        expected_streams = expected_streams.filter(ExpectedStream.name == filter_stream)

    return jsonify({'expected_streams': [e.asdict() for e in expected_streams]})


@app.route('/expected/<int:expected_id>')
def expected_by_id(expected_id):
    expected_stream = app.session.query(ExpectedStream).get(expected_id)
    if expected_stream:
        return jsonify(expected_stream.asdict())

    abort(http_client.NOT_FOUND)


@app.route('/expected/<int:expected_id>', methods=['PATCH'])
def update_expected_by_id(expected_id):
    expected_stream = app.session.query(ExpectedStream).get(expected_id)
    if expected_stream:
        patch = request.json
        # if an ID is passed, verify it matches the query id
        if 'id' in patch:
            if expected_id != patch['id']:
                abort(http_client.BAD_REQUEST)
        if 'expected_rate' in patch:
            expected_stream.expected_rate = patch['expected_rate']
        if 'warn_interval' in patch:
            expected_stream.warn_interval = patch['warn_interval']
        if 'fail_interval' in patch:
            expected_stream.fail_interval = patch['fail_interval']
        app.session.commit()
        return jsonify(expected_stream.asdict())

    abort(http_client.NOT_FOUND)


@app.route('/deployed/<int:deployed_id>')
def deployed_by_id(deployed_id):
    deployed_stream = app.session.query(DeployedStream).get(deployed_id)
    if deployed_stream:
        return jsonify(deployed_stream.asdict())

    abort(http_client.NOT_FOUND)


@app.route('/deployed/<int:deployed_id>', methods=['PATCH'])
def update_deployed_by_id(deployed_id):
    deployed_stream = app.session.query(DeployedStream).get(deployed_id)
    if deployed_stream:
        patch = request.json
        # if an ID is passed, verify it matches the query id
        if 'id' in patch:
            if deployed_id != patch['id']:
                abort(http_client.BAD_REQUEST)
        if 'expected_rate' in patch:
            deployed_stream.expected_rate = patch['expected_rate']
        if 'warn_interval' in patch:
            deployed_stream.warn_interval = patch['warn_interval']
        if 'fail_interval' in patch:
            deployed_stream.fail_interval = patch['fail_interval']
        app.session.commit()
        return jsonify(deployed_stream.asdict())

    abort(http_client.NOT_FOUND)


@app.route('/stream')
def get_streams():
    filter_status = request.args.get('status')
    filter_refdes = request.args.get('refdes')
    filter_method = request.args.get('method')
    filter_stream = request.args.get('stream')

    return jsonify(get_status_by_stream(app.session, filter_refdes, filter_method, filter_stream, filter_status))


@app.route('/stream/<int:deployed_id>')
def get_stream(deployed_id):
    status = get_status_by_stream_id(app.session, deployed_id)
    if status:
        return jsonify(status)

    abort(http_client.NOT_FOUND)


@app.route('/instrument')
def get_instruments():
    filter_status = request.args.get('status')
    filter_refdes = request.args.get('refdes')
    filter_method = request.args.get('method')
    filter_stream = request.args.get('stream')

    return jsonify(get_status_by_instrument(app.session, filter_refdes=filter_refdes, filter_method=filter_method,
                                            filter_stream=filter_stream, filter_status=filter_status))


@app.route('/instrument/<int:refdes_id>')
def get_instrument(refdes_id):
    return jsonify(get_status_by_instrument(app.session, filter_refdes_id=refdes_id))


@app.route('/stream/<int:deployed_id>/disable')
def disable_by_id(deployed_id):
    deployed = app.session.query(DeployedStream).get(deployed_id)
    if deployed:
        deployed.disable()
        app.session.commit()

    return jsonify(get_status_by_stream_id(app.session, deployed_id))


@app.route('/stream/<int:deployed_id>/enable')
def enable_by_id(deployed_id):
    deployed = app.session.query(DeployedStream).get(deployed_id)
    if deployed:
        deployed.enable()
        app.session.commit()

    return jsonify(get_status_by_stream_id(app.session, deployed_id))


@app.route('/instrument/<refdes>/disable')
def disable_by_refdes(refdes):
    deployed = app.session.query(DeployedStream).filter(DeployedStream.reference_designator == refdes)
    for each in deployed:
        each.disable()
    app.session.commit()

    return jsonify(get_status_by_instrument(app.session, filter_refdes=refdes))


@app.route('/instrument/<refdes>/enable')
def enable_by_refdes(refdes):
    deployed = app.session.query(DeployedStream).filter(DeployedStream.reference_designator == refdes)
    for each in deployed:
        each.enable()
    app.session.commit()

    return jsonify(get_status_by_instrument(app.session, filter_refdes=refdes))


@app.route('/resample')
def run_resample():
    # get a datetime object representing this HOUR
    now = datetime.datetime.utcnow().replace(second=0, minute=0)
    # get a datetime object representing this HOUR - 24
    twenty_four_ago = now - datetime.timedelta(hours=24)
    # get a datetime object representing this HOUR - 48
    fourty_eight_ago = now - datetime.timedelta(hours=48)

    for deployed_stream in DeployedStream.query:
        app.logger.error('Resampling %s', deployed_stream)
        # resample all count data from now-48 to now-24 to 1 hour
        resample_stream_count(app.session, deployed_stream.id, fourty_eight_ago, twenty_four_ago, 3600)
        app.session.commit()
    return 'DONE'
