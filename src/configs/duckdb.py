from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.configs.environment import env
from src.logger import logger

DATABASE_PATH = f"{env._local_database_folder / env.DATABASE_NAME}.duckdb"
DATABASE_URL = f"duckdb:///{DATABASE_PATH}"
logger.info(f"DUCKDB DATABASE URL: {DATABASE_URL}")

# -------------------------------------
engine = create_engine(
    DATABASE_URL,
    echo=env.DEBUG_MODE,
    # Note: DuckDB doesn't support traditional connection pooling
    # These pool settings may not apply or could cause issues
    # pool_size=32,
    # max_overflow=64,
    # pool_timeout=60,
    # pool_recycle=1800,
    # pool_pre_ping=True,
    connect_args={"read_only": False},
)
Session = sessionmaker(bind=engine)
session = Session()
# native_conn = session.connection().connection.connection  # 👈 native duckdb.Connection


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
