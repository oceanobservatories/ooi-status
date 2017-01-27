import time


class StatusEnum:
    OPERATIONAL = 'operational'
    DEGRADED = 'degraded'
    FAILED = 'failed'
    NOT_TRACKED = 'not tracked'


class StatusMessage(object):
    def __init__(self, refdes, stream, uid, elapsed, previous_status, stream_status, interval, instrument_status=None):
        self.refdes = refdes
        self.stream = stream
        self.uid = uid
        self.elapsed = elapsed
        self.previous_status = previous_status
        self.stream_status = stream_status
        self.interval = interval
        self.instrument_status = instrument_status
        self.created_time = time.time() * 1000

    def as_dict(self):
        return {
            '@class': self.event_class,
            'assetUid': self.asset_uid,
            'eventType': self.event_type,
            'eventName': self.event_name,
            'status': self.status,
            'severity': 0,
            'reason': self.reason,
            'location': self.location,
            'eventStartTime': self.event_time,
            'notes': self.notes,
            'dataSource': self.data_source,
        }

    def __repr__(self):
        return repr(self.as_dict())

    @property
    def event_class(self):
        return '.AssetStatusEvent'

    @property
    def event_type(self):
        return 'ASSET_STATUS'

    @property
    def data_source(self):
        return 'AUTO: data monitor service'

    @property
    def event_name(self):
        return self.stream

    @property
    def severity(self):
        return 0

    @property
    def status(self):
        return self.instrument_status

    @property
    def reason(self):
        if self.stream_status == StatusEnum.OPERATIONAL:
            return 'data interval within range (%s)' % self.interval
        if self.stream_status in [StatusEnum.DEGRADED, StatusEnum.FAILED]:
            return 'data interval threshold exceeded (%s > %s)' % (self.elapsed, self.interval)
        return ''

    @property
    def location(self):
        return None

    @property
    def event_time(self):
        return self.created_time

    @property
    def asset_uid(self):
        return self.uid

    @property
    def notes(self):
        return 'changed from %s to %s' % (self.previous_status, self.stream_status)
