from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sqlalchemy.orm import create_session

from src.configs.environment import env
from src.repositories.clickhouse.event import EventRepository

def create_database():
    DATABASE_URL = f"clickhouse://{env.CLICKHOUSE_USERNAME}:{quote_plus(env.CLICKHOUSE_PASSWORD)}@{env.CLICKHOUSE_SERVER}"

    engine = create_engine(DATABASE_URL)
    session = create_session(engine)
    query = f"CREATE DATABASE {env.DATABASE_NAME};"
    session.execute(text(query))


def main():
    repo = EventRepository()
    repo.create(drop=True)
    repo.inspect()


if __name__ == "__main__":
    create_database()
    main()