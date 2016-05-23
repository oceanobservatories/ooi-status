import json
import datetime

from logging import getLogger
from threading import Thread

from kombu.mixins import ConsumerMixin
from kombu import Connection, Queue
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .model.status_model import PortCount, ReferenceDesignator

log = getLogger(__name__)


class AmqpStatsClient(ConsumerMixin):
    def __init__(self, url, queue, engine):
        self.engine = engine
        self.session_factory = sessionmaker(bind=engine, autocommit=True)
        self.session = self.session_factory()
        self.connection = Connection(url)
        self.queue = Queue(name=queue, channel=self.connection)
        self._refdes_cache = {}

    def _get_or_create_refdes(self, reference_designator):
        if reference_designator not in self._refdes_cache:
            refdes = self.session.query(ReferenceDesignator).filter(
                ReferenceDesignator.name == reference_designator).first()
            if refdes is None:
                refdes = ReferenceDesignator(name=reference_designator)
                self.session.add(refdes)
                self.session.flush()
            self._refdes_cache[reference_designator] = refdes
        return self._refdes_cache[reference_designator]

    def get_consumers(self, Consumer, channel):
        return [
            Consumer([self.queue], callbacks=[self.on_message])
        ]

    def on_message(self, body, message):
        data = json.loads(body)
        bytes_in = data.get('bytes_in', 0)
        bytes_out = data.get('bytes_out', 0)
        collected = data.get('end_time')
        collected = datetime.datetime.utcfromtimestamp(collected)
        elapsed = data.get('elapsed', 0)
        refdes = data.get('reference_designator')
        adds = data.get('adds', 0)
        clients = data.get('num_clients', {}).get('client', 0)
        if clients > 0 and adds == 0 and bytes_in != (1.0 * bytes_out / clients):
            log.error('differing in/out rates: %d %d %d', bytes_in, bytes_out, clients)

        with self.session.begin():
            refdes_obj = self._get_or_create_refdes(refdes)
            pc = PortCount()
            pc.reference_designator = refdes_obj
            pc.collected_time = collected
            pc.byte_count = bytes_in
            pc.seconds = elapsed
            self.session.add(pc)
        message.ack()

    def start_thread(self):
        t = Thread(target=self.run)
        t.setDaemon(True)
        t.start()
        return t


if __name__ == '__main__':
    engine = create_engine('postgresql+psycopg2://monitor:monitor@localhost')
    a = AmqpStatsClient('amqp://', 'port_agent_stats', engine)
    t = a.start_thread()
    t.join()
