from sqlalchemy import text

from src.logger import logger
from src.repositories.sqlite.base import BaseRepository
from src.schemas.sql.token import Token as SQL_Token
from src.schemas.python.token import Token as Python_Token


class TokenRepository(BaseRepository):
    def __init__(self):
        super().__init__(
            sql_schema=SQL_Token,
            python_schema=Python_Token,
        )

    def _create(self, table_name: str = None):
        table_name = table_name or self.table_name

        query = f"""
            CREATE TABLE IF NOT EXISTS '{table_name}'
            (
                address   TEXT,
                name      TEXT,
                symbol    TEXT,
                decimals  UINT,

                updated_time       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (address)
            );
        """
        self.db.execute(text(query))
        self.db.commit()
        logger.info(f"✅ Created table '{table_name}'!")
