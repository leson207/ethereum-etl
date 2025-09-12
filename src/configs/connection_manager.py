from src.configs.clickhouse import session as clickhouse_connection
from src.configs.duckdb import session as duckdb_connection
from src.configs.kafka_conn import admin, producer


class ConnectionManager:
    DUCKDB_CONNECTION = duckdb_connection
    CLICKHOUSE_CONNECTION = clickhouse_connection
    KAFKA_PRODUCER = producer
    KAFKA_ADMIN = admin

    def close(self):
        # close all connection
        pass