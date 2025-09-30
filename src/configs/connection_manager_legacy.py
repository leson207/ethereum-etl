import inspect
from src.logger import logger
from src.configs.environment import env
from src.utils.enumeration import ExporterType


class ConnectionManager:
    def __init__(self):
        self.nats_conn = None
        self.jetstream = None
        self.sqlite_init()
        self.clickhouse_init()
        self.mapper = {
            "nats": self.nats_init,
            "kafka": self.kafka_init,
        }

    async def init(self, exporter_types):
        self.exporter_types = exporter_types
        for exporter_type in self.exporter_types:
            func = self.mapper.get(exporter_type)
            if not func:
                logger.info(f"No init function for {exporter_type}!")
                continue
            
            result = func()
            if inspect.isawaitable(result):
                await result

    def clickhouse_init(self):
        from urllib.parse import quote_plus

        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        from src.logger import logger

        DATABASE_URL = f"clickhouse://{env.CLICKHOUSE_USERNAME}:{quote_plus(env.CLICKHOUSE_PASSWORD)}@{env.CLICKHOUSE_SERVER}/{env.DATABASE_NAME}"
        logger.info(f"CLICKHOUSE DATABASE URL: {DATABASE_URL}")

        # -------------------------------------
        engine = create_engine(
            DATABASE_URL,
            echo=env.DEBUG_MODE,
            pool_size=32,
            max_overflow=64,
            pool_timeout=60,
            pool_recycle=1800,  # Recycle connections after 30 minutes
            pool_pre_ping=True,
            connect_args={"connect_timeout": 60},  # 60 second connection timeout
        )
        Session = sessionmaker(bind=engine)
        self.clickhouse_conn = Session()

    def sqlite_init(self):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        from src.logger import logger

        DATABASE_PATH = f"{env._local_database_folder / env.DATABASE_NAME}.sqlite"
        DATABASE_URL = f"sqlite:///{DATABASE_PATH}"
        logger.info(f"SQLITE DATABASE URL: {DATABASE_URL}")

        # -------------------------------------
        engine = create_engine(DATABASE_URL, echo=env.DEBUG_MODE)
        Session = sessionmaker(bind=engine)
        self.sqlite_conn = Session()

    async def kafka_init(self):
        import orjson
        from kafka import KafkaAdminClient, KafkaProducer

        self.kafka_producer = KafkaProducer(
            bootstrap_servers=env.KAFKA_SERVER,
            value_serializer=lambda v: orjson.dumps(v).encode("utf-8"),
        )
        self.kafka_admin = KafkaAdminClient(bootstrap_servers=env.KAFKA_SERVER)

    async def nats_init(self):
        import nats

        from src.configs.environment import env
        from src.logger import logger

        self.nats_conn = await nats.connect(env.NATS_SERVER)
        self.jetstream = self.nats_conn.jetstream()

        await self.jetstream.add_stream(
            name=env.DATABASE_NAME, subjects=[f"{env.NETWORK}.*"]
        )
        logger.info(f"Created stream '{env.DATABASE_NAME}'")

    async def close(self):
        if ExporterType.CLICKHOUSE in self.exporter_types:
            self.clickhouse_conn.close()
        if ExporterType.SQLITE in self.exporter_types:
            self.sqlite_conn.close()
        if ExporterType.KAFKA in self.exporter_types:
            self.kafka_producer.close()
            self.kafka_admin.close()
        if ExporterType.NATS in self.exporter_types:
            await self.nats_conn.drain()


connection_manager = ConnectionManager()
