from sqlalchemy import text

from src.logger import logger
from src.repositories.sqlite.base import BaseRepository
from src.schemas.sql.raw_receipt import RawReceipt as SQL_RawReceipt


class RawReceiptRepository(BaseRepository):
    def __init__(self):
        super().__init__(sql_schema=SQL_RawReceipt)

    def _create(self, table_name: str = None):
        table_name = table_name or self.table_name

        query = f"""
            CREATE TABLE IF NOT EXISTS '{table_name}'
            (
                block_number INT,
                transaction_hash TEXT,
                data JSON,

                updated_time DATETIME DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (block_number, transaction_hash)
            );
        """
        self.db.execute(text(query))
        self.db.commit()
        logger.info(f"✅ Created table '{table_name}'!")
