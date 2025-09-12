from sqlalchemy import Column, String

from src.schemas.sql.base import EntityMeta


class Account(EntityMeta):
    __tablename__ = "account"

    address = Column(String, primary_key=True)
