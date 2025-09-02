from sqlalchemy import text

from src.logger import logger
from src.repositories.duckdb.base import BaseRepository
from src.schemas.sql.log import Log as SQL_Log
from src.schemas.python.log import Log as Python_Log


class LogRepository(BaseRepository):
    def __init__(self):
        super().__init__(
            sql_schema=SQL_Log,
            python_schema=Python_Log,
        )

    def _create(self, table_name: str = None):
        table_name = table_name or self.table_name

        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            (
                address            TEXT,
                topics             JSON,
                data               TEXT,

                block_hash         TEXT,
                block_number       UBIGINT,
                block_timestamp    UBIGINT,

                transaction_hash   TEXT,
                transaction_index  UBIGINT,

                log_index          UBIGINT,
                removed            BOOLEAN,

                updated_time       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (transaction_hash, log_index)
            );
        """
        self.db.execute(text(query))
        self.db.commit()
        logger.info(f"✅ Created table '{table_name}'!")
