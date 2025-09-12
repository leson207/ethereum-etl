from contextlib import contextmanager
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.configs.environment import env
from src.logger import logger

DATABASE_URL = f"clickhouse://{env.CLICKHOUSE_USERNAME}:{quote_plus(env.CLICKHOUSE_PASSWORD)}@{env.CLICKHOUSE_SERVER}/{env.DATABASE_NAME}"
logger.info(f"CLICKHOUSE DATABASE URL: {DATABASE_URL}")

# -------------------------------------
engine = create_engine(
    DATABASE_URL,
    echo=env.DEBUG_MODE,
    pool_size=32,
    max_overflow=64,
    pool_timeout=60,
    pool_recycle=1800,  # Recycle connections after 30 minutes
    pool_pre_ping=True,
    connect_args={"connect_timeout": 60},  # 60 second connection timeout
)
Session = sessionmaker(bind=engine)
session = Session()
# native_conn = session.connection().connection.connection


# -------------------------------------
@contextmanager
def get_db_connection():
    db = Session()

    try:
        yield db
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Database transaction failed: {e}")
        raise
    finally:
        db.close()
