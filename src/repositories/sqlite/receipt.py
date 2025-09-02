from sqlalchemy import text

from src.logger import logger
from src.repositories.sqlite.base import BaseRepository
from src.schemas.sql.receipt import Receipt as SQL_Receipt
from src.schemas.python.receipt import Receipt as Python_Receipt


class ReceiptRepository(BaseRepository):
    def __init__(self):
        super().__init__(
            sql_schema=SQL_Receipt,
            python_schema=Python_Receipt,
        )

    def _create(self, table_name: str = None):
        table_name = table_name or self.table_name

        query = f"""
            CREATE TABLE IF NOT EXISTS '{table_name}'
            (
                block_hash           TEXT,
                block_number         UBIGINT,

                contract_address     TEXT,
                cumulative_gas_used  UBIGINT,
                from_address         TEXT,
                gas_used             UBIGINT,
                effective_gas_price  UBIGINT,
                log_count            UBIGINT,
                logs_bloom           TEXT,
                status               BOOLEAN,
                to_address           TEXT,
                transaction_hash     TEXT,
                transaction_index    UBIGINT,
                type                 UBIGINT,

                updated_time         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (transaction_hash)
            );
        """
        self.db.execute(text(query))
        self.db.commit()
        logger.info(f"✅ Created table '{table_name}'!")
