import orjson
import time

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

from src.logger import logger


class KafkaExporter:
    def __init__(self, server, topic):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=server,
            value_serializer=lambda v: orjson.dumps(v).encode("utf-8"),
        )
        self.admin = KafkaAdminClient(bootstrap_servers=server)

    def exist(self):
        topics = self.admin.list_topics()
        return self.topic in topics

    def _delete(self):
        self.admin.delete_topics([self.topic])
        time.sleep(0.1)
        logger.info(f"Deleted topic {self.topic}")

    def _create(self):
        topic = NewTopic(name=self.topic, num_partitions=1, replication_factor=1)
        self.admin.create_topics([topic])
        time.sleep(0.1)
        logger.info(f"Created topic {self.topic}")

    def create(self, exist_ok=True):
        if not exist_ok and self.exist():
            self._delete()
            self._create()
        elif exist_ok and not self.exist():
            self._create()

    def export(self, items: list[dict]):
        for item in items:
            self.producer.send(topic=self.topic, value=item)

        self.producer.flush()

    def close(self):
        self.producer.close()
        self.admin.close()
