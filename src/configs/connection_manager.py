import inspect

from src.configs.environment import env
from src.logger import logger
from src.utils.enumeration import Exporter


class ConnectionManager:
    def __init__(self):
        self.conn = {}
        self.mapper = {Exporter.SQLITE: self.init_sqlite}

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
        logger.info(f"SQLITE DATABASE URL: {DATABASE_URL}")

        # -------------------------------------
        engine = create_engine(DATABASE_URL, echo=env.DEBUG_MODE)
        Session = sessionmaker(bind=engine)
        self.conn[Exporter.SQLITE] = Session()

    async def close(self):
        for exporter in self.exporters:
            match exporter:
                case Exporter.SQLITE:
                    self.conn[Exporter.SQLITE].close()
                case _:
                    raise


connection_manager = ConnectionManager()
