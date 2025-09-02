from sqlalchemy import text

from src.logger import logger
from src.repositories.sqlite.base import BaseRepository
from src.schemas.sql.raw_block import RawBlock as SQL_RawBlock
from src.schemas.python.raw_block import RawBlock as Python_RawBlock


class RawBlockRepository(BaseRepository):
    def __init__(self):
        super().__init__(
            sql_schema=SQL_RawBlock,
            python_schema=Python_RawBlock,
        )

    def _create(self, table_name: str = None):
        table_name = table_name or self.table_name

        query = f"""
            CREATE TABLE IF NOT EXISTS '{table_name}'
            (
                block_number UBIGINT,
                included_transaction BOOLEAN,
                data JSON,

                updated_time DATETIME DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (block_number, included_transaction)
            );
        """
        self.db.execute(text(query))
        self.db.commit()
        logger.info(f"✅ Created table '{table_name}'!")
