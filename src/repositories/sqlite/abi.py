from sqlalchemy import text

from src.logger import logger
from src.repositories.sqlite.base import BaseRepository
from src.schemas.sql.abi import ABI as SQL_ABI
from src.schemas.python.abi import ABI as Python_ABI


class AbiRepository(BaseRepository):
    def __init__(self):
        super().__init__(
            sql_schema=SQL_ABI,
            python_schema=Python_ABI,
        )

    def _create(self, table_name: str = None):
        table_name = table_name or self.table_name

        query = f"""
            CREATE TABLE IF NOT EXISTS '{table_name}'
            (
                text_signature  TEXT,
                hex_signature   TEXT,
                data            JSON,

                updated_time       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (text_signature,hex_signature)
            );
        """
        self.db.execute(text(query))
        self.db.commit()
        logger.info(f"✅ Created table '{table_name}'!")
