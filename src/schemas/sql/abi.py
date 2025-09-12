from sqlalchemy import Column, String, JSON

from src.schemas.sql.base import EntityMeta


class ABI(EntityMeta):
    __tablename__ = "abi"

    text_signature = Column(String, primary_key=True)
    hex_signature = Column(String, primary_key=True)
    data = Column(JSON)