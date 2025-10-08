from sqlalchemy import text

from src.logger import logger
from src.repositories.sqlite.base import BaseRepository
from src.schemas.sql.pool import Pool as SQL_Pool


class PoolRepository(BaseRepository):
    def __init__(self):
        super().__init__(sql_schema=SQL_Pool)

    def _create(self, table_name: str = None):
        table_name = table_name or self.table_name

        query = f"""
            CREATE TABLE IF NOT EXISTS '{table_name}'
            (
                address    TEXT,
                token0_address  TEXT,
                token1_address  TEXT,
                token0_balance  HUGEUINT,
                token1_balance  HUGEUINT,
                token0_price    DOUBLE,
                token1_price    DOUBLE,

                updated_time       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (address)
            );
        """
        self.db.execute(text(query))
        self.db.commit()
        logger.info(f"✅ Created table '{table_name}'!")
