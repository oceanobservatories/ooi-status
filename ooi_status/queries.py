from datetime import datetime, timedelta
from operator import and_

import numpy as np
import pandas as pd
from sqlalchemy import func

from ooi_status.model.status_model import ReferenceDesignator, ExpectedStream, Counts, DeployedStream


def build_counts_subquery(session, timestamp, stream_id=None):
    subquery = session.query(Counts.stream_id,
                             func.max(Counts.particle_count).label('particle_count'),
                             func.max(Counts.timestamp).label('timestamp'))
    subquery = subquery.group_by(Counts.stream_id).filter(Counts.timestamp < timestamp)
    if stream_id:
        subquery = subquery.filter(Counts.stream_id == stream_id)
    return subquery.subquery()


def get_status_query(session, base_time, filter_refdes=None, filter_method=None, filter_stream=None, stream_id=None):
    filter_constraints = []
    if filter_refdes:
        filter_constraints.append(ReferenceDesignator.name.like('%%%s%%' % filter_refdes))
    if filter_method:
        filter_constraints.append(ExpectedStream.method.like('%%%s%%' % filter_method))
    if filter_stream:
        filter_constraints.append(ExpectedStream.name.like('%%%s%%' % filter_stream))

    five_ago = base_time - timedelta(minutes=5)
    day_ago = base_time - timedelta(days=1)

    # subquery to get the latest count and timestamp
    now_subquery = build_counts_subquery(session, base_time, stream_id)
    five_min_subquery = build_counts_subquery(session, five_ago, stream_id)
    one_day_subquery = build_counts_subquery(session, day_ago, stream_id)

    # Overall query, joins the three above subqueries with the DeployedStream table to produce our data
    query = session.query(DeployedStream,
                          now_subquery.c.timestamp,
                          now_subquery.c.particle_count,
                          five_min_subquery.c.particle_count,
                          one_day_subquery.c.particle_count)
    query = query.join(now_subquery).join(five_min_subquery).join(one_day_subquery)

    # Apply any filter constraints if supplied
    if filter_constraints:
        query = query.filter(and_(*filter_constraints))

    return query


def get_hourly_rates(session, stream_id):
    query = session.query(Counts).filter(Counts.stream_id == stream_id)
    counts_df = pd.read_sql_query(query.statement, query.session.bind)
    counts_df['particle_count'] = counts_df.particle_count.diff()
    counts_df['elapsed'] = counts_df.timestamp.diff() / np.timedelta64(1, 's')
    counts_df['rate'] = counts_df.particle_count / counts_df.elapsed
    counts_df = counts_df.set_index('timestamp')
    return counts_df[['rate']].resample('1H').mean()