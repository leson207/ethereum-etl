from pydantic import BaseModel


class EthPrice(BaseModel):
    timestamp: int
    price: float