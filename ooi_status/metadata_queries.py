import datetime

import pandas as pd
from ooi_data.postgres import model
from sqlalchemy import func, not_, or_

from .get_logger import get_logger

from api import app

log = get_logger(__name__)

NOT_EXPECTED = app.config['DATA_NOT_EXPECTED']
MISSING = app.config['DATA_MISSING']
PRESENT = app.config['DATA_PRESENT']
SPARSE1 = app.config['DATA_SPARSE_1']
SPARSE2 = app.config['DATA_SPARSE_2']
SPARSE3 = app.config['DATA_SPARSE_3']

data_categories = {
    NOT_EXPECTED: {'color': app.config['COLOR_NOT_EXPECTED']},
    MISSING: {'color': app.config['COLOR_MISSING']},
    PRESENT: {'color': app.config['COLOR_PRESENT']},
    SPARSE1: {'color': app.config['COLOR_SPARSE_1']},
    SPARSE2: {'color': app.config['COLOR_SPARSE_2']},
    SPARSE3: {'color': app.config['COLOR_SPARSE_3']}
}

SPARSITY_MIN = app.config['SPARSE_DATA_MIN']
SPARSITY_MID = app.config['SPARSE_DATA_MID']
SPARSITY_MAX = app.config['SPARSE_DATA_MAX']

EVEN_DEPLOYMENT = app.config['COLOR_EVEN_DEPLOYMENT']
ODD_DEPLOYMENT = app.config['COLOR_ODD_DEPLOYMENT']


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
            # identify rows that have sparse data => row has embedded gap in data
            df['interval'] = df['last'] - df['first']                       # time interval of each row
            df['mean_sep'] = df['interval'] / df['count']                   # mean separation of data points in row
            df['sparse'] = df['mean_sep'] > pd.to_timedelta(overall_interval,'s')                # row has sparse data
            df['pre_gap'] = (df['first'] - df['last_last']) > pd.to_timedelta(threshold,'s')     # gap in data before row

            # parallel dataframe identifying potential gaps
            missing = (df['pre_gap'] | df['sparse'])

            gap = (df['first'] - df['last_last']).astype('m8[s]')
            last = df['last'].iloc[-1]

            # step through each deployment any gaps *inside* the deployment bounds
            # if deployment data exists, otherwise return all found gaps
            gaps_df = df[missing]
            last_first = df['first'].iloc[0]

            # if the data falls short of the lower bound, mark a gap at the start
            if last_first > lower_bound:
                available.append((lower_bound, MISSING, last_first))

            # create spans for gaps and sparse data
            for row in gaps_df.itertuples(index=False):
                if row.pre_gap:
                    if last_first < row.last_last:
                        # only report current row if it is an interval
                        available.append((last_first, PRESENT, row.last_last))
                    # report a data gap before the current row
                    available.append((row.last_last, MISSING, row.first))
                    last_first = row.first
                else:
                    if last_first < row.first:
                        # special handling -- only report data before if valid
                        available.append((last_first, PRESENT, row.first))
                    # determine and report sparsity level of current row
                    sparseness = compute_sparseness(row, overall_interval)
                    available.append((row.first, sparseness, row.last))
                    last_first = row.last

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
                # row has data, determine sparseness
                sparseness = compute_sparseness(row, overall_interval)
                available.append((first, sparseness, last))

    return available

def compute_sparseness(row,ds_sep):
    """
    Computes the "sparseness" of a sparse row. There are three levels of sparseness:
    SPARSE1: SPARSITY_MIN <= row.mean_sep / ds_sep < SPARSITY_MID
    SPARSE2: SPARSITY_MID <= row.mean_sep / ds_sep < SPARSITY_MAX
    SPARSE3: SPARSITY_MAX <= row.mean_sep / ds_sep
    :param row: The dataset row containing the sparse data
    :param ds_sep: average separation of data points in the dataset
    :return: The appropriate "sparsness" level
    """
    # calculate the row's data density ratio
    if 'mean_sep' in row:
        sep_ratio = row.mean_sep / pd.to_timedelta(ds_sep, 's')
    else:
        # calculate the density from the row
        interval = row.last - row.first
        mean_sep = interval / row.count
        sep_ratio = mean_sep / pd.to_timedelta(ds_sep, 's')

    # in case this method is called on a row that isn't sparse, default to having data present.
    ret_val = PRESENT

    if sep_ratio >= SPARSITY_MAX:
        ret_val = SPARSE3
    elif sep_ratio >= SPARSITY_MID:
        ret_val = SPARSE2
    elif sep_ratio >= SPARSITY_MIN:
        ret_val = SPARSE1
    return ret_val

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
    else:
        filters.append(not_(model.StreamMetadatum.method.like('bad%')))
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

