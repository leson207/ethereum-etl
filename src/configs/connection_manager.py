import inspect

from src.configs.environment import env
from src.logger import logger
from src.utils.enumeration import Exporter


class ConnectionManager:
    def __init__(self):
        self.conn = {}
        self.mapper = {
            "rpc": self.init_rpc,
            "memgraph": self.init_memgraph,
            Exporter.SQLITE: self.init_sqlite,
            Exporter.CLICKHOUSE: self.init_clickhouse,
            Exporter.NATS: self.init_nats,
        }

    def __getitem__(self, key):
        return self.conn[key]

    async def init(self, exporters):
        self.exporters = exporters
        for exporter in self.exporters:
            result = self.mapper[exporter]()
            if inspect.isawaitable(result):
                await result

    def init_sqlite(self):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        DATABASE_PATH = f"{env._local_database_folder / env.DATABASE_NAME}.sqlite"
        DATABASE_URL = f"sqlite:///{DATABASE_PATH}"

        engine = create_engine(DATABASE_URL, echo=env.DEBUG_MODE)
        Session = sessionmaker(bind=engine)
        self.conn[Exporter.SQLITE] = Session()

        logger.info(f"SQLITE DATABASE URL: {DATABASE_URL}")

    async def init_nats(self):
        import nats

        self.conn["nats"] = await nats.connect(env.NATS_SERVER)
        self.conn["jetstream"] = self.conn["nats"].jetstream()

        await self.conn[
            "jetstream"
        ].add_stream(
            name=env.DATABASE_NAME, subjects=[f"{env.NETWORK}.*"]
        )  # TODO: this is create exprestion (like create databaes) check if it should be here

        logger.info(f"Created stream '{env.DATABASE_NAME}'")

    def init_clickhouse(self):
        from urllib.parse import quote_plus

        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        DATABASE_URL = f"clickhouse://{env.CLICKHOUSE_USERNAME}:{quote_plus(env.CLICKHOUSE_PASSWORD)}@{env.CLICKHOUSE_SERVER}/{env.DATABASE_NAME}"

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
        self.conn[Exporter.CLICKHOUSE] = Session()

        logger.info(f"CLICKHOUSE DATABASE URL: {DATABASE_URL}")

    async def init_rpc(self):
        from src.clients.rpc_client import RpcClient

        self.conn["rpc"] = RpcClient()
        res = await self.conn["rpc"].get_web3_client_version()
        logger.info(f"Web3 Client Version: {res[0]['result']}")

    async def init_memgraph(self):
        from neo4j import AsyncGraphDatabase

        self.conn["memgraph"] = AsyncGraphDatabase.driver(
            env.MEMGRAPH_SERVER,
            auth=(env.MEMGRAPH_USERNAME, env.MEMGRAPH_PASSWORD),
            max_connection_pool_size=50,
            connection_acquisition_timeout=45.0,
            liveness_check_timeout=300.0,
            max_connection_lifetime=1800.0,
        )
        await self.conn["memgraph"].verify_connectivity()
        logger.info(f"MEMGRAPH DATABASE URL: {env.MEMGRAPH_SERVER}")

    async def close(self):
        for exporter in self.exporters:
            match exporter:
                case Exporter.SQLITE:
                    self.conn[exporter].close()
                case Exporter.CLICKHOUSE:
                    self.conn[exporter].close()
                case Exporter.NATS:
                    await self.conn[exporter].drain()
                case "rpc":
                    await self.conn[exporter].close()
                case "memgraph":
                    await self.conn["memgraph"].close()
                case _:
                    raise


# from contextlib import contextmanager
# @contextmanager
# def get_db_connection():
#     db = Session()

#     try:
#         yield db
#         db.commit()
#     except Exception as e:
#         db.rollback()
#         logger.error(f"Database transaction failed: {e}")
#         raise
#     finally:
#         db.close()


connection_manager = ConnectionManager()
