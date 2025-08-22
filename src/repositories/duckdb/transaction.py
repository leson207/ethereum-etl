from sqlalchemy import text

from src.logger import logger
from src.repositories.duckdb.base import BaseRepository
from src.schemas.duckdb.transaction import Transaction as SQL_Transaction
from src.schemas.python.transaction import Transaction as Python_Transaction


class TransactionRepository(BaseRepository):
    def __init__(self):
        super().__init__(
            sql_schema=SQL_Transaction,
            python_schema=Python_Transaction,
        )

    def _create(self, table_name: str = None):
        table_name = table_name or self.table_name

        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            (
                transaction_type UBIGINT,
                nonce UBIGINT,
                gas UBIGINT,

                max_fee_per_gas UBIGINT,
                max_priority_fee_per_gas UBIGINT,
                max_fee_per_blob_gas UBIGINT,

                to_address TEXT,
                from_address TEXT,

                value UHUGEINT,
                input TEXT,

                transaction_hash TEXT,
                block_hash TEXT,
                block_number UBIGINT,
                transaction_index UBIGINT,

                gas_price UBIGINT,

                blob_versioned_hashes JSON,

                updated_time DATETIME DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (transaction_hash, )
            );
        """
        self.db.execute(text(query))
        self.db.commit()
        logger.info(f"✅ Created table '{table_name}'!")
