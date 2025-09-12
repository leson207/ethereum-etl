from sqlalchemy import DECIMAL, Column, Integer, String

from src.schemas.sql.base import EntityMeta


class Token(EntityMeta):
    __tablename__ = "token"

    address = Column(String, primary_key=True)
    name = Column(String)
    symbol = Column(String)
    decimals = Column(Integer)
    total_supply = Column(DECIMAL)
