import logging
import requests

from ooi_status.get_logger import get_logger

log = get_logger(__name__, logging.INFO)


class EventNotifier(object):
    """
    Status Event Notifier service - creates status events based on status changes
    """

    def __init__(self, session, base_url, query_port=12587):
        self.session = session
        self.base_url = '%s:%d/' % (base_url, query_port)
        self.query_url = '%s:%d/status/query' % (base_url, query_port)
        self.post_url = '%s:%d/events/postto' % (base_url, query_port)

    def post_event(self, uid, body):
        """
        Post status event to the uFrame service
        :param uid:   Asset UID (would like to change this to reference designator)
        :param body:  JSON formatted text body to send
        :return: response from requests call
        """
        url = '%s/%s' % (self.post_url, uid)
        log.debug('POST: %s: %r', url, body)
        r = requests.post(url, json=body)
        log.debug('RESPONSE: (%d) %r', r.status_code, r.content)
        return r
