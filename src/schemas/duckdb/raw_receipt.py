from sqlalchemy import Column, types

from src.schemas.duckdb.base import EntityMeta


class RawReceipt(EntityMeta):
    __tablename__ = "raw_receipt"

    block_number = Column(types.String, primary_key=True)
    data = Column(types.JSON)