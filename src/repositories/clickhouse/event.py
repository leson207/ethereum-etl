from sqlalchemy import text

from src.logger import logger
from src.repositories.clickhouse.base import BaseRepository
from src.schemas.sql.event import Event as SQL_Event


class EventRepository(BaseRepository):
    def __init__(self):
        super().__init__(sql_schema=SQL_Event)
    
    def _drop(self, table_name: str = None):
        table_name = table_name or self.table_name

        self.db.execute(text(f"DROP TABLE IF EXISTS {table_name}_nats;"))
        logger.info(f"✅ Dropped table '{table_name}_nats'!")

        self.db.execute(text(f"DROP TABLE IF EXISTS {table_name};"))
        logger.info(f"✅ Dropped table '{table_name}'!")

        self.db.execute(text(f"DROP TABLE IF EXISTS {table_name}_materialized;"))
        logger.info(f"✅ Dropped table '{table_name}_materialized'!")

        self.db.commit()

    def _create(self, table_name: str = None, physic_talbe_only: bool = False):
        table_name = table_name or self.table_name

        #--------------------------------------------------------
        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name}_nats
            (
                type              String,
                dex               String,
                pool_address      String,

                amount0_in        String,
                amount1_in        String,
                amount0_out       String,
                amount1_out       String,

                transaction_hash  String,
                log_index         String,
                block_number      String,

                updated_time      DateTime DEFAULT now()
            )
            ENGINE = NATS
            SETTINGS nats_url = 'nats://nats-server:4222',
                    nats_subjects = 'ethereum.event',
                    nats_format = 'JSONEachRow',
                    date_time_input_format = 'best_effort';
        """
        if not physic_talbe_only:
            self.db.execute(text(query))

        #--------------------------------------------------------
        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            (
                type              String,
                dex               String,
                pool_address      String,

                amount0_in        String,
                amount1_in        String,
                amount0_out       String,
                amount1_out       String,

                transaction_hash  String,
                log_index         String,
                block_number      String,

                updated_time      DateTime DEFAULT now()
            )
            ENGINE = ReplacingMergeTree(updated_time)
            ORDER BY (block_number, transaction_hash, log_index)
            PRIMARY KEY (block_number, transaction_hash, log_index);
        """
        self.db.execute(text(query))

        #--------------------------------------------------------
        query = f"""
            CREATE MATERIALIZED VIEW {table_name}_materialized to {table_name}
            AS SELECT * FROM {table_name}_nats;
        """
        if not physic_talbe_only:
            self.db.execute(text(query))

        #--------------------------------------------------------
        self.db.commit()
        logger.info(f"✅ Created table '{table_name}'!")
