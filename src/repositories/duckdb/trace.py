from sqlalchemy import text

from src.logger import logger
from src.repositories.duckdb.base import BaseRepository
from src.schemas.duckdb.trace import Trace as SQL_Trace
from src.schemas.python.trace import Trace as Python_Trace


class TraceRepository(BaseRepository):
    def __init__(self):
        super().__init__(
            sql_schema=SQL_Trace,
            python_schema=Python_Trace,
        )

    def _create(self, table_name: str = None):
        table_name = table_name or self.table_name

        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            (
                action               JSON,

                block_hash           TEXT,
                block_number         UBIGINT,

                error                TEXT,
                result               JSON,

                subtraces            UBIGINT,
                trace_address        JSON,

                transaction_hash     TEXT,
                transaction_position UBIGINT,

                type                 TEXT,

                updated_time         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (transaction_hash, transaction_position, subtraces)
            );
        """
        self.db.execute(text(query))
        self.db.commit()
        logger.info(f"✅ Created table '{table_name}'!")
