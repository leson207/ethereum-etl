from sqlalchemy import text

from src.logger import logger
from src.repositories.duckdb.base import BaseRepository
from src.schemas.sql.withdrawal import Withdrawal as SQL_Withdrawal
from src.schemas.python.withdrawal import Withdrawal as Python_Withdrawal


class WithdrawalRepository(BaseRepository):
    def __init__(self):
        super().__init__(
            sql_schema=SQL_Withdrawal,
            python_schema=Python_Withdrawal,
        )

    def _create(self, table_name: str = None):
        table_name = table_name or self.table_name

        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            (
                block_hash TEXT,
                block_number UBIGINT,

                index UBIGINT,
                validator_index UBIGINT,
                address TEXT,
                amount UBIGINT,

                updated_time DATETIME DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (block_number, block_hash, index, )
            );
        """
        self.db.execute(text(query))
        self.db.commit()
        logger.info(f"✅ Created table '{table_name}'!")
