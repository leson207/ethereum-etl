from sqlalchemy import Column, types

from src.schemas.sql.base import EntityMeta


class RawReceipt(EntityMeta):
    __tablename__ = "raw_receipt"

    block_number = Column(types.Integer, primary_key=True)
    transaction_hash = Column(types.String, primary_key=True)
    data = Column(types.JSON)