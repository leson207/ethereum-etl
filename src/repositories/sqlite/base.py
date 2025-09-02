from datetime import datetime
from pathlib import Path
from typing import Dict, List, Literal, Type

import sqlalchemy as sa
from sqlalchemy import text
from tabulate import tabulate

from src.configs.sqlite import session
from src.configs.environment import env
from src.logger import logger


class BaseRepository:
    def __init__(
        self,
        sql_schema: Type,
        python_schema: Type,
    ):
        self.db = session
        self.sql_schema = sql_schema
        self.python_schema = python_schema
        self.table_name = sql_schema.__tablename__
        self.primary_keys = [col.name for col in sql_schema.__table__.primary_key]

        # self.db.execute(text("PRAGMA enable_progress_bar;"))
        self.db.execute(text("PRAGMA enable_optimizer;"))

    def inspect(self):
        # Show existing tables
        res = self.db.execute(text("SELECT name, type FROM sqlite_master WHERE type IN ('table','view');"))
        print(tabulate(res.fetchall(), headers=res.keys(), tablefmt="pretty"))

        # Show row data
        res = self.db.execute(text(f"SELECT * FROM {self.table_name} LIMIT 3;"))
        print(tabulate(res.fetchall(), headers=res.keys(), tablefmt="pretty"))

        # Show row count
        res = self.db.execute(
            text(f"SELECT COUNT(*) as total_row FROM {self.table_name};")
        )
        print(tabulate(res.fetchall(), headers=res.keys(), tablefmt="pretty"))

        # Show table schema
        res = self.db.execute(text(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{self.table_name}';"))
        print(tabulate(res.fetchall(), headers=res.keys(), tablefmt="pretty"))

        res = self.db.execute(text(f"PRAGMA table_info('{self.table_name}');"))
        print(tabulate(res.fetchall(), headers=res.keys(), tablefmt="pretty"))

    def _create(self, table_name: str = None):
        table_name = table_name or self.table_name
        # Create table
        query = f"""
            CREATE TABLE IF NOT EXISTS '{table_name}'
            (
                id UUID PRIMARY KEY,
                name TEXT,
                age INT,

                updated_time DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """
        self.db.execute(text(query))
        logger.info(f"✅ Created table '{table_name}'!")

        self.db.commit()

    def _drop(self, table_name: str = None):
        table_name = table_name or self.table_name

        # drop table
        self.db.execute(text(f"DROP TABLE IF EXISTS '{table_name}';"))
        logger.info(f"✅ Dropped table '{table_name}'!")

        self.db.commit()

    def _backup(self, table_name: str = None):
        # 1. Determine backup table name
        if not table_name:
            timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
            table_name = f"{self.table_name}_{timestamp}"

        # 2. Create backup table
        self._create(table_name=table_name)

        # 3. Insert all data from source table into backup table
        query = f"""
            INSERT INTO {table_name}
            SELECT * FROM {self.table_name}
        """
        self.db.execute(text(query))
        self.db.commit()
        logger.info(f"✅ Backed up '{self.table_name}' → '{table_name}' successfully!")

    def _restore(self, table_name: str = None):
        query = f"""
            INSERT OR IGNORE INTO {self.table_name}
            SELECT * FROM {table_name}
        """
        self.db.execute(text(query))
        self.db.commit()
        logger.info(f"♻ Restored data from '{table_name}' into '{self.table_name}'")

    def restore(self, mode: Literal["latest", "all"] = "latest"):
        query = f"""
            SELECT table_name FROM information_schema.tables WHERE table_name LIKE '{self.table_name}_%_%_%_%_%_%'
            ORDER BY strptime(substr(table_name, -19), '%Y_%m_%d_%H_%M_%S') DESC
        """
        rows = self.db.execute(text(query)).fetchall()

        if mode == "latest":
            num_tables = 1
        elif mode == "all":
            num_tables = len(rows)

        for i in range(num_tables):
            self._restore(rows[i][0])

    def _drop_backup(self):
        query = f"""
            SELECT table_name FROM information_schema.tables WHERE table_name LIKE '{self.table_name}_%_%_%_%_%_%'
            ORDER BY strptime(substr(table_name, -19), '%Y_%m_%d_%H_%M_%S') DESC
        """
        rows = self.db.execute(text(query)).fetchall()
        for row in rows:
            table_name = row[0]
            self._drop(table_name)

    def create(
        self, exist_ok: bool = True, backup: bool = False, restore: bool = False
    ):
        if backup:
            self._backup()

        if not exist_ok:
            self._drop()

        self._create()

        if restore:
            self.restore(mode="latest")

    def insert(self, data: List[Dict], deduplicate: str = None):
        if not data:
            logger.warning(
                f"No data provided to insert into {self.table_name}. Skipping."
            )
            return

        if deduplicate == "ignore":
            prefix = "OR IGNORE"
        elif deduplicate == "replace":
            prefix = "OR REPLACE"
        else:
            prefix = ""

        for i in data:
            for key, value in i.items():
                if not isinstance(value, bool) and isinstance(value, int):
                    i[key] = str(value) # SQLITE overflow vbig int value

        stmt = sa.insert(self.sql_schema).prefix_with(prefix)
        self.db.execute(stmt, data)
        self.db.commit()

        logger.info(f"Inserted {len(data)} rows into {self.table_name} table.")

    def export(
        self,
        folder_path: Path = env._local_data_folder,
        file_name: str = None,
    ):
        file_name = f"{self.table_name}.csv" or file_name
        full_path = folder_path / file_name
        self.db.execute(text(f"COPY {self.table_name} TO '{full_path}';"))
        logger.info(f"✅ Exported data from '{self.table_name}' to '{full_path}'")

    def exist(self):
        query = f"SELECT * FROM information_schema.tables WHERE table_name = '{self.table_name}';"
        res = self.db.execute(text(query)).fetchall()

        return res is not None

    def query(self):
        pass

    def delete(self):
        pass

    def update(self):
        pass

    def count(self):
        query = f"SELECT COUNT() FROM {self.table_name}"
        num_rows = self.db.execute(text(query)).scalar()
        return num_rows

    def free_space(self, keep_ratio: float):
        total = self.count()
        num_delete = int((1 - keep_ratio) * total)
        query = f"""
            WITH to_delete AS (
                SELECT ROWID AS rid
                FROM token
                ORDER BY updated_time
                LIMIT {num_delete}
            )
            DELETE FROM token
            WHERE ROWID IN (SELECT rid FROM to_delete);
            """
        self.db.execute(text(query))
        self.db.commit()
        logger.info(
            f"Total: {total} rows. Deleted: {num_delete} rows. Remain: {total - num_delete} rows."
        )

    def truncate(self):
        query = f"TRUNCATE {self.table_name}"
        num_rows = self.db.execute(text(query)).scalar()
        self.db.commit()
        logger.info(f"Truncated table '{self.table_name}'. Total: {num_rows} rows!")

    def reclaim_space(self):
        query = "CHECKPOINT"
        self.db.execute(text(query))
        self.db.commit()
