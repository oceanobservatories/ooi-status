import datetime

import pandas as pd
from ooi_data.postgres import model
from sqlalchemy import func
from sqlalchemy import or_

from .get_logger import get_logger


log = get_logger(__name__)

NOT_EXPECTED = 'Not Expected'
MISSING = 'Missing'
PRESENT = 'Present'

data_categories = {
    NOT_EXPECTED: { 'color': '#ffffff'},
    MISSING: {'color': '#d9534d'},
    PRESENT: {'color': '#5cb85c'}
}

EVEN_DEPLOYMENT = '#0073cf'
ODD_DEPLOYMENT = '#cf5c00'


def get_data(session, subsite, node, sensor, method, stream, lower_bound, upper_bound):
    """
    Fetch the specified parameter metadata as a pandas dataframe
    :param session: sqlalchemy session object
    :param subsite: subsite portion of reference designator (e.g. RS03AXPS)
    :param node: node portion of reference designator (e.g. SF01A)
    :param sensor: sensor portion of reference designator (e.g. 01-CTDPFA101)
    :param method: stream delivery method
    :param stream: stream name
    :param lower_bound: datetime object representing the lower time bound of this query
    :param upper_bound: datetime object representing the upper time bound of this query
    :return: pandas DataFrame containing all partition metadata records matching the above criteria
    """
    pm = model.PartitionMetadatum
    filters = [
        pm.subsite == subsite,
        pm.node == node,
        pm.sensor == sensor,
        pm.method == method,
        pm.stream == stream,
        pm.last > lower_bound,
        pm.first < upper_bound
    ]

    fields = [
        pm.bin,
        pm.first,
        pm.last,
        pm.count
    ]

    query = session.query(*fields).filter(*filters).order_by(pm.bin)
    df = pd.read_sql_query(query.statement, query.session.bind, index_col='bin')
    return df


def find_data_spans(session, subsite, node, sensor, method, stream, lower_bound, upper_bound):
    """
    Find all data spans for the specified data.
    :param session: sqlalchemy session object
    :param subsite: subsite portion of reference designator (e.g. RS03AXPS)
    :param node: node portion of reference designator (e.g. SF01A)
    :param sensor: sensor portion of reference designator (e.g. 01-CTDPFA101)
    :param method: stream delivery method
    :param stream: stream name
    :param lower_bound: datetime object representing the lower time bound of this query
    :param upper_bound: datetime object representing the upper time bound of this query
    :return:
    """
    df = get_data(session, subsite, node, sensor, method, stream, lower_bound, upper_bound)
    available = []

    if df.size > 0:
        # calculate the mean interval between samples based on the supplied bounds
        span = (upper_bound - lower_bound).total_seconds()
        count = df['count'].sum()
        overall_interval = 0
        threshold = span / 1000.0
        if count:
            overall_interval = span / count

        # if the sample interval is less than 1/1000 the time span
        # find gaps
        if overall_interval < threshold and df.size > 30:
            df['last_last'] = df['last'].shift(1)
            gap = (df['first'] - df['last_last']).astype('m8[s]')
            last = df['last'].iloc[-1]

            # step through each deployment any gaps *inside* the deployment bounds
            # if deployment data exists, otherwise return all found gaps
            gaps_df = df[gap > threshold]
            last_first = df['first'].iloc[0]

            # if the data falls short of the lower bound, mark a gap at the start
            if last_first > lower_bound:
                available.append((lower_bound, MISSING, last_first))

            # create spans for all gaps
            for row in gaps_df.itertuples(index=False):
                available.append((last_first, PRESENT, row.last_last))
                available.append((row.last_last, MISSING, row.first))
                last_first = row.first

            # create an available span for the tail end
            available.append((last_first, PRESENT, last))

            # if the end of the data falls short of the upper bound, mark a gap at the end
            if last < upper_bound:
                available.append((last, MISSING, upper_bound))

        # sample interval is greater than gap threshold
        # plot actual data spans instead
        else:
            for row in df.itertuples(index=False):
                # we can't display spans which are too small
                # pad segments smaller than 2 x threshold
                if (row.last - row.first).total_seconds() < (2*threshold):
                    first = row.first - datetime.timedelta(seconds=threshold)
                    last = row.first + datetime.timedelta(seconds=threshold)
                else:
                    first = row.first
                    last = row.last

                available.append((first, PRESENT, last))

    return available


def filter_spans(spans, deploy_data):
    """
    Given an ordered list of spans and an ordered list of deployment bounds,
    filter all spans to inside the bounds of the deployments.
    :param spans: tuples representing (start, span_type, stop)
    :param deploy_data: tuples representing (start, deployment number, stop)
    :return: spans adjusted to fit inside deployment bounds
    """
    index = 0
    new_spans = []
    for start, _, stop in deploy_data:
        for span_start, span_type, span_stop, in spans:
            if span_start > stop:
                if index > 0:
                    index -= 1
                break

            if span_start < start:
                span_start = start

            if span_stop > stop:
                span_stop = stop

            new_spans.append((span_start, span_type, span_stop))
            index += 1
    return new_spans


def find_instrument_availability(session, refdes, method=None, stream=None, lower_bound=None, upper_bound=None):
    """
    :param session: sqlalchemy session object
    :param refdes: Instrument reference designator
    :param method: stream delivery method
    :param stream: stream name
    :param lower_bound: datetime object representing the lower time bound of this query
    :param upper_bound: datetime object representing the upper time bound of this query
    :return: visavail.js compatible representation of the data availability for this query
    """
    subsite, node, sensor = refdes.split('-', 2)
    now = datetime.datetime.utcnow()
    if upper_bound is None or upper_bound > now:
        upper_bound = now

    if lower_bound is None:
        lower_bound = session.query(func.min(model.Xdeployment.eventstarttime).label('first')).first().first

    avail = []
    deploy_data = []
    categories = {}

    # Fetch deployment bounds
    query = get_deployments(session, subsite, node, sensor, lower_bound=lower_bound, upper_bound=upper_bound)
    for index, deployment in enumerate(query):
        start = deployment.eventstarttime
        stop = deployment.eventstoptime

        if start is None or start < lower_bound:
            start = lower_bound

        if stop is None or stop > upper_bound:
            stop = upper_bound

        name = 'Deployment: %d' % deployment.deploymentnumber
        deploy_data.append((start, name, stop))
        if index % 2 == 0:
            categories[name] = {'color': EVEN_DEPLOYMENT}
        else:
            categories[name] = {'color': ODD_DEPLOYMENT}

    if deploy_data:
        avail.append({'measure': 'Deployments', 'data': deploy_data,
                      'categories': categories})

        # update bounds based on deployment data
        deployment_lower_bound = min((x[0] for x in deploy_data))
        deployment_upper_bound = max((x[2] for x in deploy_data))

        if lower_bound is None or lower_bound < deployment_lower_bound:
            log.info('Adjusting lower bound to minimum deployment value: %r -> %r', lower_bound, deployment_lower_bound)
            lower_bound = deployment_lower_bound

        if upper_bound is None or upper_bound > deployment_upper_bound:
            log.info('Adjusting upper bound to maximum deployment value: %r -> %r', upper_bound, deployment_upper_bound)
            upper_bound = deployment_upper_bound

    # Fetch all possible streams
    query = session.query(model.StreamMetadatum)

    filters = [
        model.StreamMetadatum.subsite == subsite,
        model.StreamMetadatum.node == node,
        model.StreamMetadatum.sensor == sensor
    ]

    if method:
        filters.append(model.StreamMetadatum.method == method)
    if stream:
        filters.append(model.StreamMetadatum.stream == stream)

    query = query.filter(*filters)

    # Fetch gaps for all streams found
    for row in query:
        gaps = find_data_spans(session, subsite, node, sensor, row.method, row.stream, lower_bound, upper_bound)
        gaps = filter_spans(gaps, deploy_data)
        if gaps:
            avail.append({
                'measure': '%s %s' % (row.method, row.stream),
                'data': gaps,
                'categories': data_categories
            })
        else:
            avail.append({
                'measure': '%s %s' % (row.method, row.stream),
                'data': [(lower_bound, NOT_EXPECTED, lower_bound)],
                'categories': data_categories
            })

    return avail


def get_all_streams(session):
    """
    :param session: sqlalchemy session object
    :return: generator yielding the reference designator, delivery method, stream name, start and stop for all streams
    """
    for row in session.query(model.StreamMetadatum):
        yield row.refdes, row.method, row.stream, row.count, row.stop


def get_active_streams(session):
    """
    Return all streams which are within an active deployment
    :param session: sqlalchemy session object
    :return: (StreamMetadatum, TimeDelta(since last particle), String(Asset UID))
    """
    now = datetime.datetime.utcnow()
    for sm, assetid, uid in session.query(
        model.StreamMetadatum,
        model.Xdeployment.sassetid,
        model.Xasset.uid
    ).filter(
        model.StreamMetadatum.subsite == model.Xdeployment.subsite,
        model.StreamMetadatum.node == model.Xdeployment.node,
        model.StreamMetadatum.sensor == model.Xdeployment.sensor,
        model.StreamMetadatum.method.in_(['telemetered', 'streamed']),
        model.Xdeployment.sassetid == model.Xasset.assetid,
        or_(
            model.Xdeployment.eventstoptime.is_(None),
            model.Xdeployment.eventstoptime > now
        )
    ):
        yield sm, now - sm.last, uid


def get_deployments(session, subsite, node, sensor, lower_bound=None, upper_bound=None):
    """
    Query which returns all known deployments for the specified instrument
    :param session: sqlalchemy session object
    :param subsite: subsite portion of reference designator (e.g. RS03AXPS)
    :param node: node portion of reference designator (e.g. SF01A)
    :param sensor: sensor portion of reference designator (e.g. 01-CTDPFA101)
    :param lower_bound: datetime object representing the lower time bound of this query
    :param upper_bound: datetime object representing the upper time bound of this query
    :return: sqlalchemy query object representing this query
    """
    filters = [
        model.Xdeployment.subsite == subsite,
        model.Xdeployment.node == node,
        model.Xdeployment.sensor == sensor
    ]

    if lower_bound:
        filters.append(
            or_(
                model.Xdeployment.eventstoptime > lower_bound,
                model.Xdeployment.eventstoptime.is_(None)
            ))
    if upper_bound:
        filters.append(model.Xdeployment.eventstarttime < upper_bound)

    return session.query(model.Xdeployment).filter(*filters)


def get_current_deployment(session, subsite, node, sensor):
    """
    Query which returns all known deployments for the specified instrument
    :param session: sqlalchemy session object
    :param subsite: subsite portion of reference designator (e.g. RS03AXPS)
    :param node: node portion of reference designator (e.g. SF01A)
    :param sensor: sensor portion of reference designator (e.g. 01-CTDPFA101)
    :return: sqlalchemy query object representing this query
    """
    filters = [
        model.Xdeployment.subsite == subsite,
        model.Xdeployment.node == node,
        model.Xdeployment.sensor == sensor
    ]

    return session.query(model.Xdeployment).filter(*filters).order_by(model.Xdeployment.deploymentnumber.desc()).first()


def get_uid_from_refdes(session, refdes):
    subsite, node, sensor = refdes.split('-', 2)
    d = get_current_deployment(session, subsite, node, sensor)
    return d.xinstrument.asset.uid

