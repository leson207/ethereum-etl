from sqlalchemy import text

from src.logger import logger
from src.repositories.sqlite.base import BaseRepository
from src.schemas.sql.raw_trace import RawTrace as SQL_RawTrace
from src.schemas.python.raw_trace import RawTrace as Python_RawTrace


class RawTraceRepository(BaseRepository):
    def __init__(self):
        super().__init__(
            sql_schema=SQL_RawTrace,
            python_schema=Python_RawTrace,
        )

    def _create(self, table_name: str = None):
        table_name = table_name or self.table_name

        query = f"""
            CREATE TABLE IF NOT EXISTS '{table_name}'
            (
                block_number TEXT,
                data JSON,

                updated_time DATETIME DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (block_number)
            );
        """
        self.db.execute(text(query))
        self.db.commit()
        logger.info(f"✅ Created table '{table_name}'!")
