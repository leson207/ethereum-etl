from sqlalchemy import text

from src.logger import logger
from src.repositories.sqlite.base import BaseRepository
from src.schemas.sql.event import Event as SQL_Event


class EventRepository(BaseRepository):
    def __init__(self):
        super().__init__(sql_schema=SQL_Event)

    def _create(self, table_name: str = None):
        table_name = table_name or self.table_name

        query = f"""
            CREATE TABLE IF NOT EXISTS '{table_name}'
            (
                type              TEXT,
                dex               TEXT,
                pool_address      TEXT,
                amount0_in        BIGINT,
                amount1_in        BIGINT,
                amount0_out       BIGINT,
                amount1_out       BIGINT,
                transaction_hash  TEXT,
                log_index         UBIGINT,
                block_number      UBIGINT,

                block_timestamp   UINT,
                eth_price         FLOAT,
                
                token0_address    TEXT,
                token0_name       TEXT,
                token0_symbol     TEXT,
                token0_decimals   TEXT,
                
                token1_address    TEXT,
                token1_name       TEXT,
                token1_symbol     TEXT,
                token1_decimals   TEXT,

                updated_time       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (block_number, transaction_hash, log_index)
            );
        """
        self.db.execute(text(query))
        self.db.commit()
        logger.info(f"✅ Created table '{table_name}'!")
