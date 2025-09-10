from sqlalchemy import text

from src.logger import logger
from src.repositories.sqlite.cache import CacheRepository


class CacheService:
    def __init__(self):
        self.client = CacheRepository()
        self.client.create(exist_ok=True)

    def get(self, key):
        query = f"SELECT * FROM {self.client.table_name} WHERE key='{key}' LIMIT 1;"
        res = self.client.db.execute(text(query))
        res = res.fetchall()
        if res:
            return res[0][1] # key, value, created_time

        return None

    def set(self, key, value):
        row = [{"key": key, "value": value}]
        self.client.insert(row)

    def delete(self, key):
        query = f"DELETE FROM {self.client.table_name} WHERE key='{key}';"
        self.client.db.execute(text(query))
        self.client.db.commit()
        logger.debug(f"Delete {key} from cache!")

cache_service = CacheService()