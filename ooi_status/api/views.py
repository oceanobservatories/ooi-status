import six.moves.http_client as http_client
from flask import jsonify, request, send_file
from sqlalchemy import and_
from werkzeug.exceptions import abort

from ..api import app
from ..model.status_model import ExpectedStream, DeployedStream, NotifyAddress
from ..queries import (get_status_by_instrument, get_status_by_stream,
                       get_status_by_stream_id, plot_stream_rates_buf,
                       plot_port_rates_buf, create_daily_digest_html,
                       create_weekly_digest_html)


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


@app.route('/stream/<int:deployed_id>/plot')
def get_plot(deployed_id):
    title, buf = plot_stream_rates_buf(app.session, deployed_id)
    return send_file(buf, attachment_filename='%s.png' % title, mimetype='image/png')


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


@app.route('/instrument/<int:refdes_id>/plot')
def plot_instrument_rate(refdes_id):
    title, buf = plot_port_rates_buf(app.session, refdes_id)
    return send_file(buf, attachment_filename='%s.png' % title, mimetype='image/png')


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


@app.route('/email', methods=['GET'])
def get_emails():
    emails = app.session.query(NotifyAddress)
    return jsonify({'addresses': [e.asdict() for e in emails]})


@app.route('/email/<email_addr>/<email_type>', methods=['PUT'])
def add_email(email_addr, email_type):
    notify = NotifyAddress(email_addr=email_addr, email_type=email_type)
    app.session.add(notify)
    app.session.commit()
    return ''


@app.route('/email/<email_addr>/<email_type>', methods=['DELETE'])
def del_email(email_addr, email_type):
    notify = app.session.query(NotifyAddress).filter(and_(NotifyAddress.email_addr == email_addr,
                                                          NotifyAddress.email_type == email_type)).first()
    if notify:
        app.session.delete(notify)
        app.session.commit()
        return ''

    abort(http_client.NOT_FOUND)


@app.route('/dailydigest')
def get_daily_digest():
    return create_daily_digest_html(app.session)


@app.route('/dailydigest/<site>')
def get_daily_digest_site(site):
    return create_daily_digest_html(app.session, site=site)


@app.route('/weeklydigest')
def get_weekly_digest():
    return create_daily_digest_html(app.session)


@app.route('/weeklydigest/<site>')
def get_weekly_digest_site(site):
    return create_weekly_digest_html(app.session, site=site)
