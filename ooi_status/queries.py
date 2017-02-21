import logging
from collections import Counter
from datetime import timedelta, datetime

import pandas as pd
from ooi_data.postgres.model import ExpectedStream, DeployedStream, PortCount, ReferenceDesignator
from sqlalchemy.sql.elements import and_

from .get_logger import get_logger
from .status_message import StatusEnum

log = get_logger(__name__, logging.INFO)


def get_status_query(session, filter_refdes=None, filter_method=None, filter_status=None, filter_stream=None):
    query = session.query(DeployedStream).join(ExpectedStream, ReferenceDesignator)

    filter_constraints = []
    if filter_refdes:
        filter_constraints.append(ReferenceDesignator.name.like('%%%s%%' % filter_refdes))
    if filter_method:
        filter_constraints.append(ExpectedStream.method.like('%%%s%%' % filter_method))
    if filter_stream:
        filter_constraints.append(ExpectedStream.name.like('%%%s%%' % filter_stream))
    if filter_status:
        filter_constraints.append(DeployedStream.status.like('%%%s%%' % filter_status))

    query = query.filter(*filter_constraints)
    return query


def resample_port_count(session, refdes_id, counts_df, seconds):
    fields = ['byte_count', 'seconds']
    if not counts_df.empty:
        resampled = counts_df.resample('%dS' % seconds).sum()[fields]
        # drop the old records
        session.query(PortCount).filter(PortCount.id.in_(list(counts_df.id.values))).delete(
            synchronize_session=False)
        # insert the new records
        for collected_time, byte_count, seconds in resampled.itertuples():
            sc = PortCount(reference_designator_id=refdes_id, collected_time=collected_time,
                           byte_count=byte_count, seconds=seconds)
            session.add(sc)
        return resampled


def get_port_data_rates(session, refdes_id):
    counts_df = get_port_rates_dataframe(session, refdes_id, None, None)
    if not counts_df.empty:
        counts_df = counts_df.resample('1H').mean()
        counts_df['rate'] = counts_df.byte_count / counts_df.seconds
    return counts_df


def get_status_by_instrument(session, filter_refdes=None, filter_method=None, filter_stream=None, filter_status=None):
    query = get_status_query(session,
                             filter_refdes=filter_refdes,
                             filter_method=filter_method,
                             filter_stream=filter_stream,
                             filter_status=filter_status)

    # group by reference designator
    grouped = {}
    for ds in query:
        refdes = ds.reference_designator.name
        grouped.setdefault(refdes, []).append(ds)

    out = {}
    # create a rollup status
    for refdes in grouped:
        streams = grouped[refdes]
        overall = _rollup_statuses(set(s.status for s in streams))
        out[refdes] = {
            'overall': overall,
            'status': streams
        }

    return out


def get_status_by_refdes_id(session, refdes_id):
    query = session.query(DeployedStream).join(ReferenceDesignator).filter(ReferenceDesignator.id == refdes_id)
    streams = list(query)
    overall = _rollup_statuses(set(s.status for s in streams))
    return {
        'overall': overall,
        'status': streams
    }


def get_status_by_stream(session, filter_refdes=None, filter_method=None, filter_stream=None, filter_status=None):
    return {
        'status': list(get_status_query(session, filter_refdes=filter_refdes, filter_method=filter_method,
                                        filter_stream=filter_stream, filter_status=filter_status))
    }


def get_status_by_stream_id(session, deployed_id):
    return session.query(DeployedStream).get(deployed_id).first()


#### RATES ####

def get_port_rates_dataframe(session, refdes_id, start, end):
    now = datetime.utcnow()
    if start is None:
        start = now - timedelta(days=1)
    if end is None:
        end = now
    query = session.query(PortCount).filter(and_(PortCount.reference_designator_id == refdes_id,
                                                 PortCount.collected_time >= start,
                                                 PortCount.collected_time < end)).order_by(PortCount.collected_time)
    counts_df = pd.read_sql_query(query.statement, query.session.bind, index_col='collected_time')
    counts_df['rate'] = counts_df.byte_count / counts_df.seconds
    return counts_df


def _rollup_statuses(statuses):
    if StatusEnum.FAILED in statuses:
        return StatusEnum.FAILED
    elif StatusEnum.DEGRADED in statuses:
        return StatusEnum.DEGRADED
    elif StatusEnum.OPERATIONAL in statuses:
        return StatusEnum.OPERATIONAL
    return StatusEnum.NOT_TRACKED


def _rollup_status_query(query):
    statuses = Counter((status[0] for status in query))
    rollup_status = _rollup_statuses(statuses)
    reasons = []
    for key in [StatusEnum.OPERATIONAL, StatusEnum.DEGRADED, StatusEnum.FAILED, StatusEnum.NOT_TRACKED]:
        if key in statuses:
            reasons.append('%s: %d' % (key, statuses[key]))

    rollup_reason = 'Stream statuses: ' + ', '.join(reasons)
    return rollup_status, rollup_reason


def get_rollup_status_by_id(session, refdes_id):
    query = session.query(DeployedStream.status).join(ReferenceDesignator)
    query = query.filter(ReferenceDesignator.id == refdes_id)
    return _rollup_status_query(query)


def get_rollup_status(session, refdes):
    query = session.query(DeployedStream.status).join(ReferenceDesignator)
    query = query.filter(ReferenceDesignator.name == refdes)
    return _rollup_status_query(query)
