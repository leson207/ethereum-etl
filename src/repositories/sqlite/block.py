from sqlalchemy import text

from src.logger import logger
from src.repositories.sqlite.base import BaseRepository
from src.schemas.sqlite.block import Block as SQL_Block
from src.schemas.python.block import Block as Python_Block


class BlockRepository(BaseRepository):
    def __init__(self):
        super().__init__(
            sql_schema=SQL_Block,
            python_schema=Python_Block,
        )

    def _create(self, table_name: str = None):
        table_name = table_name or self.table_name

        query = f"""
            CREATE TABLE IF NOT EXISTS '{table_name}'
            (
                number                UBIGINT,
                hash                  TEXT,
                parent_hash           TEXT,
                sha3_uncles           TEXT,
                miner                 TEXT,

                state_root            TEXT,
                transactions_root     TEXT,
                receipts_root         TEXT,

                logs_bloom            TEXT,
                difficulty            UBIGINT,
                total_difficulty      UBIGINT,

                size                  UBIGINT,
                gas_limit             UBIGINT,
                gas_used              UBIGINT,
                timestamp             UBIGINT,
                extra_data            TEXT,

                mix_hash              TEXT,
                nonce                 TEXT,

                base_fee_per_gas      UBIGINT,
                withdrawals_root      TEXT,
                withdrawal_count      UBIGINT,

                blob_gas_used         UBIGINT,
                excess_blob_gas       UBIGINT,
                parent_beacon_block_root TEXT,
                requests_hash         TEXT,

                uncles                JSON,

                transaction_count     UBIGINT,

                updated_time DATETIME DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (number, hash)
            );
        """
        
        self.db.execute(text(query))
        self.db.commit()
        logger.info(f"✅ Created table '{table_name}'!")
