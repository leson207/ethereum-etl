from sqlalchemy import Column, String, DECIMAL

from src.schemas.sql.base import EntityMeta


class Pool(EntityMeta):
    __tablename__ = "pool"

    pool_address = Column(String, primary_key=True)
    token0_address = Column(String)
    token1_address = Column(String)
    token1_balance = Column(DECIMAL)
    token0_balance = Column(DECIMAL)