# from contextlib import contextmanager

# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker

# from src.configs.environment import env
# from src.logger import logger


# DATABASE_PATH = f"{env._local_database_folder / env.DATABASE_NAME}.sqlite"
# DATABASE_URL = f"sqlite:///{DATABASE_PATH}"
# logger.info(f"SQLITE DATABASE URL: {DATABASE_URL}")

# # -------------------------------------
# engine = create_engine(
#     DATABASE_URL,
#     echo=env.DEBUG_MODE
# )
# Session = sessionmaker(bind=engine)
# session = Session()
# # native_conn = session.connection().connection.connection  # 👈 native Connection


# # -------------------------------------
# @contextmanager
# def get_db_connection():
#     db = Session()

#     try:
#         yield db
#         db.commit()
#     except Exception as e:
#         db.rollback()
#         logger.error(f"Database transaction failed: {e}")
#         raise
#     finally:
#         db.close()
