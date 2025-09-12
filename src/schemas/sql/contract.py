from sqlalchemy import Column, String

from src.schemas.sql.base import EntityMeta


class Contract(EntityMeta):
    __tablename__ = "contract"

    name = Column(String)
    address = Column(String, primary_key=True)
    abi = Column(String)
