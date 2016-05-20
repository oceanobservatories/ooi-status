import logging
from collections import Counter
from datetime import timedelta, datetime

import pandas as pd
from sqlalchemy import func
from sqlalchemy.sql.elements import and_

from ooi_status.emailnotifier import EmailNotifier
from ooi_status.get_logger import get_logger
from ooi_status.model.status_model import ExpectedStream, DeployedStream, StreamCount, StreamCondition

RATE_ACCEPTABLE_DEVIATION = 0.3
RIGHT = '&rarr;'
UP = '&uarr;'
DOWN = '&darr;'

log = get_logger(__name__, logging.INFO)


def build_counts_subquery(session, timestamp, stream_id=None, first=False):
    fields = [StreamCount.stream_id,
              func.sum(StreamCount.particle_count).label('particle_count'),
              func.sum(StreamCount.seconds).label('seconds')]

    if first:
        fields.append(func.max(StreamCount.collected_time).label('collected_time'))

    subquery = session.query(*fields).group_by(StreamCount.stream_id).filter(StreamCount.collected_time > timestamp)
    if stream_id:
        subquery = subquery.filter(StreamCount.stream_id == stream_id)
    return subquery.subquery()


def get_status_query(session, base_time, filter_refdes=None, filter_method=None,
                     filter_stream=None, stream_id=None, windows=None):
    filter_constraints = []
    if filter_refdes:
        filter_constraints.append(DeployedStream.reference_designator.like('%%%s%%' % filter_refdes))
    if filter_method:
        filter_constraints.append(ExpectedStream.method.like('%%%s%%' % filter_method))
    if filter_stream:
        filter_constraints.append(ExpectedStream.name.like('%%%s%%' % filter_stream))

    if windows is None:
        windows = [{'minutes': 5}, {'hours': 1}, {'days': 1}, {'days': 7}]

    subqueries = []
    fields = [DeployedStream]
    for index, window in enumerate(windows):
        first = index == 0
        window_time = base_time - timedelta(**window)
        window_subquery = build_counts_subquery(session, window_time, stream_id, first=first)
        subqueries.append(window_subquery)
        if first:
            fields.append(window_subquery.c.collected_time)
        fields.append(window_subquery.c.seconds)
        fields.append(window_subquery.c.particle_count)

    # Overall query, joins the three above subqueries with the DeployedStream table to produce our data
    query = session.query(*fields).join(subqueries[0])
    # all subqueries but the current time are OUTER JOIN so that we will still generate a row
    # even if no data exists
    for subquery in subqueries[1:]:
        query = query.outerjoin(subquery)

    # Apply any filter constraints if supplied
    if filter_constraints:
        query = query.join(ExpectedStream)
        query = query.filter(and_(*filter_constraints))

    return query


def resample(session, stream_id, start_time, end_time, seconds):
    # fetch all count data in our window
    query = session.query(StreamCount).filter(and_(StreamCount.stream_id == stream_id,
                                                   StreamCount.collected_time >= start_time,
                                                   StreamCount.collected_time < end_time))
    counts_df = pd.read_sql_query(query.statement, query.session.bind, index_col='collected_time')
    # resample to our new interval
    if len(counts_df) > 0:
        resampled = counts_df.resample('%dS' % seconds).sum()
        # drop the old records
        session.query(StreamCount).filter(StreamCount.id.in_(list(counts_df.id.values))).delete(synchronize_session=False)
        # insert the new records
        for collected_time, _, _, particle_count, seconds in resampled.itertuples():
            sc = StreamCount(stream_id=stream_id, collected_time=collected_time,
                             particle_count=particle_count, seconds=seconds)
            session.add(sc)
        return resampled

def resample_port_count(session, stream_id, start_time, end_time, seconds):
    # fetch all count data in our window
    query = session.query(StreamCount).filter(and_(StreamCount.stream_id == stream_id,
                                                   StreamCount.collected_time >= start_time,
                                                   StreamCount.collected_time < end_time))
    counts_df = pd.read_sql_query(query.statement, query.session.bind, index_col='collected_time')
    # resample to our new interval
    if len(counts_df) > 0:
        resampled = counts_df.resample('%dS' % seconds).sum()
        # drop the old records
        session.query(StreamCount).filter(StreamCount.id.in_(list(counts_df.id.values))).delete(
            synchronize_session=False)
        # insert the new records
        for collected_time, _, _, particle_count, seconds in resampled.itertuples():
            sc = StreamCount(stream_id=stream_id, collected_time=collected_time,
                             particle_count=particle_count, seconds=seconds)
            session.add(sc)
        return resampled


def get_data_rates(session, stream_id):
    query = session.query(StreamCount).filter(StreamCount.stream_id == stream_id)
    counts_df = pd.read_sql_query(query.statement, query.session.bind, index_col='collected_time')
    counts_df.sort_index(inplace=True)
    counts_df = counts_df.resample('1H').mean()
    counts_df['rate'] = counts_df.particle_count / counts_df.seconds
    return counts_df


def compute_rate(current_count, current_timestamp, previous_count, previous_timestamp):
    if not all([current_count, current_timestamp, previous_count, previous_timestamp]):
        return 0
    elapsed = (current_timestamp - previous_timestamp).total_seconds()
    if elapsed == 0:
        return 0
    return 1.0 * (current_count - previous_count) / elapsed


def create_status_dict(row):
    if row:
        (deployed_stream, collected_time, five_min_time, five_min_count, one_hour_time, one_hour_count,
         one_day_time, one_day_count, one_week_time, one_week_count) = row

        row_dict = deployed_stream.asdict()

        five_min_rate = five_min_count / five_min_time
        one_hour_rate = one_hour_count / one_hour_time
        one_day_rate = one_day_count / one_day_time
        one_week_rate = one_week_count / one_week_time

        rates = {
            'five_mins': five_min_rate,
            'one_hour': one_hour_rate,
            'one_day': one_day_rate,
            'one_week': one_week_rate
        }

        row_dict['last_seen'] = deployed_stream.last_seen
        row_dict['rates'] = rates

        if deployed_stream.last_seen:
            elapsed = collected_time - deployed_stream.last_seen
            elapsed_seconds = elapsed.total_seconds()
            row_dict['elapsed'] = str(elapsed)
        else:
            elapsed_seconds = 999999999

        row_dict['elapsed_seconds'] = elapsed_seconds
        row_dict['status'] = get_status(deployed_stream, elapsed_seconds, five_min_rate, one_day_rate)
        return row_dict


def get_status(deployed_stream, elapsed_seconds, five_min_rate, one_day_rate):
        if deployed_stream.disabled:
            return 'DISABLED'

        expected_rate = deployed_stream.get_expected_rate()
        warn_interval = deployed_stream.get_warn_interval()
        fail_interval = deployed_stream.get_fail_interval()

        if not any([expected_rate, fail_interval, warn_interval]):
            return 'UNTRACKED'

        if fail_interval > 0:
            if elapsed_seconds > fail_interval * 700:
                return 'DEAD'
            elif elapsed_seconds > fail_interval:
                return 'FAILED'

        if 0 < warn_interval < elapsed_seconds:
            return 'DEGRADED'

        if expected_rate > 0:
            five_min_percent = 1 - RATE_ACCEPTABLE_DEVIATION
            one_day_percent = 1 - (RATE_ACCEPTABLE_DEVIATION / (86400 / 300.0))
            five_min_thresh = expected_rate * five_min_percent
            one_day_thresh = expected_rate * one_day_percent

            if (five_min_rate < five_min_thresh) and (one_day_rate < one_day_thresh):
                return 'DEGRADED'

        return 'OPERATIONAL'


def get_status_by_instrument(session, filter_refdes=None, filter_method=None, filter_stream=None, filter_status=None):
    base_time = get_base_time()
    query = get_status_query(session, base_time, filter_refdes, filter_method, filter_stream)

    # group by reference designator
    out_dict = {}
    for row in query:
        row_dict = create_status_dict(row)
        refdes = row_dict.pop('reference_designator')
        out_dict.setdefault(refdes, []).append(row_dict)

    out = []
    # create a rollup status
    for refdes in out_dict:
        streams = out_dict[refdes]
        statuses = [stream.get('status') for stream in streams]
        if 'DEAD' in statuses:
            status = 'DEAD'
        elif 'FAILED' in statuses:
            status = 'FAILED'
        elif 'DEGRADED' in statuses:
            status = 'DEGRADED'
        elif 'OPERATIONAL' in statuses:
            status = 'OPERATIONAL'
        else:
            status = statuses[0]

        if not filter_status or filter_status == status:
            out.append({'reference_designator': refdes, 'streams': streams, 'status': status})

    counts = Counter()
    for row in out:
        counts[row['status']] += 1

    return {'counts': counts, 'instruments': out, 'num_records': len(out)}


def get_base_time():
    return datetime.utcnow().replace(second=0, microsecond=0)


def get_status_by_stream(session, filter_refdes=None, filter_method=None, filter_stream=None, filter_status=None):
    base_time = get_base_time()
    query = get_status_query(session, base_time, filter_refdes, filter_method, filter_stream)

    out = []
    for row in query:
        row_dict = create_status_dict(row)

        if not filter_status or filter_status == row_dict['status']:
            out.append(row_dict)

    counts = Counter()
    for row in out:
        counts[row['status']] += 1

    return {'counts': counts, 'streams': out, 'num_records': len(out)}


def get_status_by_stream_id(session, deployed_id, include_rates=True):
    base_time = get_base_time()
    query = get_status_query(session, base_time, stream_id=deployed_id)
    result = query.first()
    if result is not None:
        row_dict = create_status_dict(result)
        d = {'deployed': row_dict}
        if include_rates:
            counts_df = get_data_rates(session, deployed_id)
            d['rates'] = list(counts_df.rate)
            d['hours'] = list(counts_df.index)
        return d


def get_status_for_notification(session):
    base_time = get_base_time()
    query = get_status_query(session, base_time, windows=[{'minutes': 5}, {'days': 1}])
    status_dict = {}
    for row in query:
        deployed_stream, collected_time, five_min_time, five_min_count, one_day_time, one_day_count = row

        five_min_rate = five_min_count / five_min_time
        one_day_rate = one_day_count / one_day_time

        if deployed_stream.last_seen:
            elapsed = collected_time - deployed_stream.last_seen
            elapsed_seconds = elapsed.total_seconds()
        else:
            elapsed_seconds = -1

        status = get_status(deployed_stream, elapsed_seconds, five_min_rate, one_day_rate)
        condition = deployed_stream.stream_condition
        if condition is None:
            now = datetime.utcnow()
            condition = StreamCondition(deployed_stream=deployed_stream, last_status=status, last_status_time=now)
            session.add(condition)

        if condition.last_status != status:
            d = {
                'id': deployed_stream.id,
                'stream': deployed_stream.expected_stream.name,
                'method': deployed_stream.expected_stream.method,
                'last_status': condition.last_status,
                'new_status': status,
                'expected_rate': deployed_stream.get_expected_rate(),
                'warn_interval': deployed_stream.get_warn_interval(),
                'fail_interval': deployed_stream.get_fail_interval(),
                'elapsed': elapsed_seconds,
                'five_minute_rate': five_min_rate,
                'one_day_rate': one_day_rate,
                'arrow': get_arrow(condition.last_status, status),
                'color': get_color(status)
            }
            status_dict.setdefault(deployed_stream.reference_designator, []).append(d)
            condition.last_status = status

    if check_should_notify(status_dict):
        notify(status_dict)
    return {'status': status_dict}


def check_should_notify(status_dict):
    """
    Criteria for notification:

    Any entry has a current or previous status of FAILED
    10 or more streams changed state
    """
    if sum((len(v) for v in status_dict.itervalues())) > 9:
        return True

    for refdes in status_dict:
        for status in status_dict[refdes]:
            if status.last_status == 'FAILED' or status.new_status == 'FAILED':
                return True

    return False


def get_color(new_status):
    colors = {
        'DEGRADED': 'orange',
        'FAILED': 'red',
        'DEAD': 'red'
    }
    return colors.get(new_status, 'black')


def get_arrow(last_status, new_status):
    ignore = ['UNTRACKED', 'DISABLED']

    if last_status in ignore or new_status in ignore:
        return RIGHT

    weights = {
        'OPERATIONAL': 0,
        'DEGRADED': 1,
        'FAILED': 2,
        'DEAD': 3
    }
    last_weight = weights.get(last_status, 0)
    new_weight = weights.get(new_status, 0)

    if last_weight > new_weight:
        return UP
    return DOWN


def notify(status_dict):
    noreply = 'noreply@ooi.rutgers.edu'  # config
    notify_list = ['petercable+ooistatus@gmail.com', 'danmergens+ooistatus@gmail.com']  # database
    notify_subject = 'OOI STATUS CHANGE NOTIFICATION'  # config
    notifier = EmailNotifier('localhost')
    notifier.send_status(noreply, notify_list, notify_subject, status_dict)
