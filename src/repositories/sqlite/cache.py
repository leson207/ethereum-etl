from sqlalchemy import text

from src.logger import logger
from src.repositories.sqlite.base import BaseRepository
from src.schemas.python.cache import Cache as Python_Cache
from src.schemas.sql.cache import Cache as SQL_Cache


class CacheRepository(BaseRepository):
    def __init__(self):
        super().__init__(
            sql_schema=SQL_Cache,
            python_schema=Python_Cache,
        )

    def _create(self, table_name: str = None):
        table_name = table_name or self.table_name

        query = f"""
            CREATE TABLE IF NOT EXISTS '{table_name}'
            (
                KEY            TEXT,
                value          TEXT,

                updated_time   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (key)
            );
        """
        self.db.execute(text(query))
        self.db.commit()
        logger.info(f"✅ Created table '{table_name}'!")
