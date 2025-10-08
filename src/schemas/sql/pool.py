from sqlalchemy import Column, String, DECIMAL, Double

from src.schemas.sql.base import EntityMeta


class Pool(EntityMeta):
    __tablename__ = "pool"

    address = Column(String, primary_key=True)
    token0_address = Column(String)
    token1_address = Column(String)
    token0_balance = Column(DECIMAL)
    token1_balance = Column(DECIMAL)
    token0_usd_price = Column(Double)
    token1_usd_price = Column(Double)