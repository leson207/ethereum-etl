from sqlalchemy import Column, types

from src.schemas.duckdb.base import EntityMeta


class RawBlock(EntityMeta):
    __tablename__ = "raw_block"

    block_number = Column(types.Integer, primary_key=True)
    included_transaction = Column(types.Boolean, primary_key=True)
    data = Column(types.JSON)