import os
import io
import six
import jinja2
import logging
import pandas as pd
import matplotlib.pyplot as plt

from collections import Counter, OrderedDict
from datetime import timedelta, datetime
from sqlalchemy import func
from sqlalchemy.sql.elements import and_

from .emailnotifier import EmailNotifier
from .get_logger import get_logger
from .model.status_model import (ExpectedStream, DeployedStream, StreamCount,
                                 StreamCondition, PortCount, ReferenceDesignator)



RATE_ACCEPTABLE_DEVIATION = 0.3
RIGHT = '&rarr;'
UP = '&uarr;'
DOWN = '&darr;'

log = get_logger(__name__, logging.INFO)
loader = jinja2.PackageLoader('ooi_status', 'templates')
jinja_env = jinja2.Environment(loader=loader, trim_blocks=True)


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
                     filter_stream=None, stream_id=None, windows=None, filter_refdes_id=None):
    filter_constraints = []
    if filter_refdes_id:
        filter_constraints.append(DeployedStream.reference_designator_id == filter_refdes_id)
    if filter_refdes:
        filter_constraints.append(ReferenceDesignator.name.like('%%%s%%' % filter_refdes))
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
        query = query.join(ReferenceDesignator)
        query = query.filter(and_(*filter_constraints))

    return query


def resample_stream_count(session, stream_id, counts_df, seconds):
    fields = ['particle_count', 'seconds']
    if not counts_df.empty:
        resampled = counts_df.resample('%dS' % seconds).sum()[fields]
        # drop the old records
        session.query(StreamCount).filter(StreamCount.id.in_(
            list(counts_df.id.values))).delete(synchronize_session=False)
        # insert the new records
        for collected_time, particle_count, seconds in resampled.itertuples():
            sc = StreamCount(stream_id=stream_id, collected_time=collected_time,
                             particle_count=particle_count, seconds=seconds)
            session.add(sc)
        return resampled


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


def get_data_rates(session, stream_id):
    counts_df = get_stream_rates_dataframe(session, stream_id, None, None)
    if not counts_df.empty:
        counts_df = counts_df.resample('1H').mean()
        counts_df['rate'] = counts_df.particle_count / counts_df.seconds
    return counts_df


def get_port_data_rates(session, refdes_id):
    counts_df = get_port_rates_dataframe(session, refdes_id, None, None)
    if not counts_df.empty:
        counts_df = counts_df.resample('1H').mean()
        counts_df['rate'] = counts_df.byte_count / counts_df.seconds
    return counts_df


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


def get_status_by_instrument(session, filter_refdes=None, filter_method=None, filter_stream=None,
                             filter_status=None, filter_refdes_id=None):
    base_time = get_base_time()
    query = get_status_query(session, base_time, filter_refdes=filter_refdes,
                             filter_method=filter_method, filter_stream=filter_stream,
                             filter_refdes_id=filter_refdes_id)

    # group by reference designator
    out_dict = {}
    for row in query:
        row_dict = create_status_dict(row)
        refdes = row_dict.pop('reference_designator')
        refdes_id = row_dict.pop('reference_designator_id')
        out_dict.setdefault((refdes, refdes_id), []).append(row_dict)

    out = []
    # create a rollup status
    for refdes, refdes_id in out_dict:
        streams = out_dict[(refdes, refdes_id)]
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
            out.append({'reference_designator': refdes, 'reference_designator_id': refdes_id,
                        'streams': streams, 'status': status})

    if filter_refdes_id:
        if out:
            d = out[0]
            counts_df = get_port_data_rates(session, filter_refdes_id)
            if not counts_df.empty:
                d['rates'] = list(counts_df.rate)
                d['hours'] = list(counts_df.index)
            else:
                d['rates'] = []
                d['hours'] = []
            return d
        return {}

    counts = Counter()
    for row in out:
        counts[row['status']] += 1

    return {'counts': counts, 'instruments': out, 'num_records': len(out)}


def get_status_by_refdes(session, refdes_id):
    base_time = get_base_time()
    query = get_status_query(session, base_time, filter_refdes_id=refdes_id)

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

        out.append({'reference_designator': refdes, 'streams': streams, 'status': status})

    counts = Counter()
    for row in out:
        counts[row['status']] += 1

    return {'counts': counts, 'instruments': out, 'num_records': len(out)}


def get_base_time():
    return datetime.utcnow().replace(second=0, microsecond=0)


def get_status_by_stream(session, filter_refdes=None, filter_method=None, filter_stream=None, filter_status=None):
    base_time = get_base_time()
    query = get_status_query(session, base_time, filter_refdes=filter_refdes, filter_method=filter_method,
                             filter_stream=filter_stream)

    out = []
    for row in query:
        log.error(row)
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
    log.error(result)
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
    return status_dict


def check_should_notify(status_dict):
    """
    Criteria for notification:

    Any entry has a current or previous status of FAILED
    10 or more streams changed state
    """
    if sum((len(v) for v in six.itervalues(status_dict))) > 9:
        return True

    for refdes in status_dict:
        for status in status_dict[refdes]:
            if status['last_status'] == 'FAILED' or status['new_status'] == 'FAILED':
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


def get_unique_sites(session):
    site_set = set()
    for refdes in session.query(ReferenceDesignator):
        site, _ = refdes.name.split('-', 1)
        site_set.add(site)
    return site_set


#### RATES ####

def get_stream_rates_dataframe(session, stream_id, start, end):
    now = datetime.utcnow()
    if start is None:
        start = now - timedelta(days=1)
    if end is None:
        end = now
    query = session.query(StreamCount).filter(
        and_(StreamCount.stream_id == stream_id,
             StreamCount.collected_time >= start,
             StreamCount.collected_time < end)).order_by(StreamCount.collected_time)
    counts_df = pd.read_sql_query(query.statement, query.session.bind, index_col='collected_time')
    counts_df['rate'] = counts_df.particle_count / counts_df.seconds
    return counts_df


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


def plot_stream_rates(session, stream_id, filename_or_handle, start=None, end=None):
    counts_df = get_stream_rates_dataframe(session, stream_id, start, end)
    if not counts_df.empty:
        ds = session.query(DeployedStream).get(stream_id)
        title = '-'.join((ds.reference_designator.name, ds.expected_stream.name, ds.expected_stream.method))
        plot = counts_df.plot(y='rate', style='.', title=title, figsize=(10, 4), alpha=0.5, color='red', fontsize=8)
        plot.get_figure().savefig(filename_or_handle, format='png')
        plt.close()
        return title


def plot_stream_rates_buf(session, stream_id, start=None, end=None):
    buf = io.BytesIO()
    title = plot_stream_rates(session, stream_id, buf, start, end)
    buf.seek(0)
    return title, buf


def plot_port_rates(session, refdes_id, filename_or_handle, start=None, end=None):
    counts_df = get_port_rates_dataframe(session, refdes_id, start, end)
    if not counts_df.empty:
        rd = session.query(ReferenceDesignator).get(refdes_id)
        plot = counts_df.plot(y='rate', style='.', title=rd.name, figsize=(10, 4), alpha=0.5, color='red', fontsize=8)
        plot.get_figure().savefig(filename_or_handle, format='png')
        plt.close()
        return rd.name


def plot_port_rates_buf(session, refdes_id, start=None, end=None):
    buf = io.BytesIO()
    title = plot_port_rates(session, refdes_id, buf, start, end)
    buf.seek(0)
    return title, buf


def create_digest_plots(session, start, end, www_root, image_dir):
    basedir = os.path.join(www_root, image_dir, str(start.year), str(start.month), str(start.day))
    if not os.path.exists(basedir):
        os.makedirs(basedir)

    for refdes in session.query(ReferenceDesignator).order_by(ReferenceDesignator.name):
        filename = '%s.png' % refdes.name
        filepath = os.path.join(basedir, filename)
        log.info('plotting port rates: %s', filepath)
        plot_port_rates(session, refdes.id, filepath, start, end)

    for deployed in session.query(DeployedStream):
        title = '-'.join((deployed.reference_designator.name,
                          deployed.expected_stream.name,
                          deployed.expected_stream.method))
        filename = title + '.png'
        filepath = os.path.join(basedir, filename)
        log.info('plotting stream rates: %s', filepath)
        plot_stream_rates(session, deployed.id, filepath, start, end)

    return 'DONE'


def create_digest_html(session, start, end, image_dir, root_url, image_url, site):
    basedir = os.path.join(image_dir, str(start.year), str(start.month), str(start.day))
    baseurl = os.path.join(image_url, str(start.year), str(start.month), str(start.day))

    image_dict = OrderedDict()
    if site is None:
        query = session.query(ReferenceDesignator).order_by(ReferenceDesignator.name)
    else:
        query_filter = ReferenceDesignator.name.like('%%%s%%' % site)
        query = session.query(ReferenceDesignator).filter(query_filter).order_by(ReferenceDesignator.name)

    for refdes in query:
        image_dict[refdes.name] = {}
        filename = '%s.png' % refdes.name
        filepath = os.path.join(basedir, filename)
        fileurl = os.path.join(baseurl, filename)
        if os.path.exists(filepath):
            image_dict[refdes.name]['image'] = fileurl
            image_dict[refdes.name]['id'] = refdes.id

    if site is None:
        query = session.query(DeployedStream)
    else:
        query_filter = ReferenceDesignator.name.like('%%%s%%' % site)
        query = session.query(DeployedStream).join(ReferenceDesignator).filter(query_filter)

    for deployed in query:
        title = '-'.join((deployed.reference_designator.name,
                          deployed.expected_stream.name,
                          deployed.expected_stream.method))
        filename = title + '.png'
        filepath = os.path.join(basedir, filename)
        fileurl = os.path.join(baseurl, filename)
        if os.path.exists(filepath):
            deployed_dict = {
                'id': deployed.id,
                'image': fileurl
            }
            image_dict[deployed.reference_designator.name].setdefault('streams', {})[title] = deployed_dict

    now = datetime.utcnow()
    return jinja_env.get_template('daily.jinja').render(image_dict=image_dict, start=start, end=end,
                                                        root_url=root_url, now=now)


def create_daily_digest_plots(session, day=None, www_root='.', image_dir='images/daily'):
    # generate a digest for the specified day, or the previous calendar day if no day specified
    if day is None:
        end = datetime.utcnow().date()
        start = end - timedelta(days=1)
    else:
        start = day.date()
        end = start + timedelta(days=1)

    return create_digest_plots(session, start, end, www_root, image_dir)


def create_daily_digest_html(session, day=None, root_url='',
                             image_dir='images/daily', image_url='/images/daily', site=None):
    # generate a digest for the specified day, or the previous calendar day if no day specified
    if day is None:
        end = datetime.utcnow().date()
        start = end - timedelta(days=1)
    else:
        start = day.date()
        end = start + timedelta(days=1)

    return create_digest_html(session, start, end, image_dir, root_url, image_url, site)


def create_weekly_digest_plots(session, day=None, www_root='.', image_dir='images/weekly'):
    if day is None:
        end = datetime.utcnow().date()
        start = end - timedelta(days=7)
    else:
        start = day.date()
        end = start + timedelta(days=7)

    return create_digest_plots(session, start, end, www_root, image_dir)


def create_weekly_digest_html(session, day=None, root_url='',
                              image_dir='images/weekly', image_url='/images/weekly', site=None):
    if day is None:
        end = datetime.utcnow().date()
        start = end - timedelta(days=7)
    else:
        start = day.date()
        end = start + timedelta(days=7)

    return create_digest_html(session, start, end, image_dir, root_url, image_url, site)
