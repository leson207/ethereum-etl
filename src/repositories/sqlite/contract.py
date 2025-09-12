from sqlalchemy import text

from src.logger import logger
from src.repositories.sqlite.base import BaseRepository
from src.schemas.sql.contract import Contract as SQL_Contract
from src.schemas.python.contract import Contract as Python_Contract


class ContractRepository(BaseRepository):
    def __init__(self):
        super().__init__(
            sql_schema=SQL_Contract,
            python_schema=Python_Contract,
        )

    def _create(self, table_name: str = None):
        table_name = table_name or self.table_name

        query = f"""
            CREATE TABLE IF NOT EXISTS '{table_name}'
            (
                name           TEXT,
                address        TEXT,
                abi            TEXT,

                updated_time       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                PRIMARY KEY (address)
            );
        """
        self.db.execute(text(query))
        self.db.commit()
        logger.info(f"✅ Created table '{table_name}'!")
