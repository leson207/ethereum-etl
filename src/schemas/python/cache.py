from pydantic import BaseModel


class Cache(BaseModel):
    key: str
    value: str
