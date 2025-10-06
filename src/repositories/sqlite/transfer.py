from sqlalchemy import text

from src.logger import logger
from src.repositories.sqlite.base import BaseRepository
from src.schemas.sql.transfer import Transfer as SQL_Transfer


class TransferRepository(BaseRepository):
    def __init__(self):
        super().__init__(sql_schema=SQL_Transfer)

    def _create(self, table_name: str = None):
        table_name = table_name or self.table_name

        query = f"""
            CREATE TABLE IF NOT EXISTS '{table_name}'
            (
                contract_address   TEXT,
                from_address       TEXT,
                to_address         TEXT,
                value              UBIGINT,
                transaction_hash   TEXT,
                log_index          UBIGINT,
                block_number       UBIGINT,

                updated_time       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (block_number, transaction_hash, log_index)
            );
        """
        self.db.execute(text(query))
        self.db.commit()
        logger.info(f"✅ Created table '{table_name}'!")
