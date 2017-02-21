import six.moves.http_client as http_client
from dateutil.parser import parse
from flask import jsonify, request
from ooi_data.postgres.model import ExpectedStream, DeployedStream
from werkzeug.exceptions import abort

from ..api import app
from ..metadata_queries import find_instrument_availability
from ..queries import (get_status_by_instrument, get_status_by_stream,
                       get_status_by_stream_id, get_status_by_refdes_id)


@app.teardown_appcontext
def shutdown_session(exception=None):
    app.session.remove()
    app.metadata_session.remove()


@app.route('/available/<refdes>', methods=['GET'])
def available(refdes):
    filter_method = request.args.get('method')
    filter_stream = request.args.get('stream')
    start_time = request.args.get('start_time')
    stop_time = request.args.get('stop_time')

    if start_time is not None:
        start_time = parse(start_time)
    if stop_time is not None:
        stop_time = parse(stop_time)
    return jsonify({'availability': find_instrument_availability(
        app.metadata_session, refdes, filter_method, filter_stream, lower_bound=start_time, upper_bound=stop_time)})


@app.route('/expected', methods=['GET'])
def expected():
    filter_method = request.args.get('method')
    filter_stream = request.args.get('stream')
    expected_streams = app.session.query(ExpectedStream)

    if filter_method:
        expected_streams = expected_streams.filter(ExpectedStream.method == filter_method)

    if filter_stream:
        expected_streams = expected_streams.filter(ExpectedStream.name == filter_stream)

    return jsonify({'expected_streams': [e.as_dict() for e in expected_streams]})


@app.route('/expected/<int:expected_id>', methods=['GET'])
def expected_by_id(expected_id):
    expected_stream = app.session.query(ExpectedStream).get(expected_id)
    if expected_stream:
        return jsonify(expected_stream.as_dict())

    abort(http_client.NOT_FOUND)


@app.route('/expected/<int:expected_id>', methods=['PATCH'])
def update_expected_by_id(expected_id):
    def patch(expected, patch):
        # if an ID is passed, verify it matches the query id
        if 'id' in patch:
            if expected.id != patch['id']:
                abort(http_client.BAD_REQUEST)
        if 'expected_rate' in patch:
            expected.expected_rate = patch['expected_rate']
        if 'warn_interval' in patch:
            expected.warn_interval = patch['warn_interval']
        if 'fail_interval' in patch:
            expected.fail_interval = patch['fail_interval']

    expected_stream = app.session.query(ExpectedStream).get(expected_id)
    if expected_stream:
        patch(expected_stream, request.json)
        app.session.commit()
        return jsonify(expected_stream.as_dict())

    abort(http_client.NOT_FOUND)


@app.route('/deployed/<int:deployed_id>')
def deployed_by_id(deployed_id):
    deployed_stream = app.session.query(DeployedStream).get(deployed_id)
    if deployed_stream:
        return jsonify(deployed_stream.as_dict())

    abort(http_client.NOT_FOUND)


@app.route('/deployed/<int:deployed_id>', methods=['PATCH'])
def update_deployed_by_id(deployed_id):
    def patch(deployed, patch):
        if 'id' in patch:
            if deployed.id != patch['id']:
                abort(http_client.BAD_REQUEST)
        if 'expected_rate' in patch:
            deployed._expected_rate = patch['expected_rate']
        if 'warn_interval' in patch:
            deployed._warn_interval = patch['warn_interval']
        if 'fail_interval' in patch:
            deployed._fail_interval = patch['fail_interval']

    deployed_stream = app.session.query(DeployedStream).get(deployed_id)
    if deployed_stream:
        patch(deployed_stream, request.json)
        app.session.commit()
        return jsonify(deployed_stream.as_dict())

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
        return jsonify(status.as_dict())

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
    return jsonify(get_status_by_refdes_id(app.session, refdes_id))


@app.route('/stream/<int:deployed_id>/disable', methods=['PUT'])
def disable_by_id(deployed_id):
    deployed = app.session.query(DeployedStream).get(deployed_id)
    if deployed:
        deployed.disable()
        app.session.commit()

    return jsonify(get_status_by_stream_id(app.session, deployed_id))


@app.route('/stream/<int:deployed_id>/enable', methods=['PUT'])
def enable_by_id(deployed_id):
    deployed = app.session.query(DeployedStream).get(deployed_id)
    if deployed:
        deployed.enable()
        app.session.commit()

    return jsonify(get_status_by_stream_id(app.session, deployed_id))


@app.route('/instrument/<refdes>/disable', methods=['PUT'])
def disable_by_refdes(refdes):
    deployed = app.session.query(DeployedStream).filter(DeployedStream.reference_designator == refdes)
    for each in deployed:
        each.disable()
    app.session.commit()

    return jsonify(get_status_by_instrument(app.session, filter_refdes=refdes))


@app.route('/instrument/<refdes>/enable', methods=['PUT'])
def enable_by_refdes(refdes):
    deployed = app.session.query(DeployedStream).filter(DeployedStream.reference_designator == refdes)
    for each in deployed:
        each.enable()
    app.session.commit()

    return jsonify(get_status_by_instrument(app.session, filter_refdes=refdes))
