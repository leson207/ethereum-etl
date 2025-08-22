from kafka import KafkaAdminClient, KafkaProducer
from src.configs.environment import env
import orjson

producer = KafkaProducer(
            bootstrap_servers=env.KAFKA_SERVER,
            value_serializer=lambda v: orjson.dumps(v).encode("utf-8"),
        )
admin = KafkaAdminClient(bootstrap_servers=env.KAFKA_SERVER)