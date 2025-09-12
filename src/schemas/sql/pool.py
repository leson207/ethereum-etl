from sqlalchemy import Column, String

from src.schemas.sql.base import EntityMeta


class Pool(EntityMeta):
    __tablename__ = "pool"

    pool_address = Column(String, primary_key=True)
    token0_address = Column(String)
    token1_address = Column(String)
