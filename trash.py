from src.services.pool_service import PoolService
from src.clients.rpc_client import RpcClient
from src.configs.environment import env
import asyncio

client = RpcClient(uris=env.PROVIER_URIS)
service = PoolService(client)
res = asyncio.run(service.get_token1_address("0xa29381815cfa5ca485c7b7a1bf9dde05bf257a44"))
print(res)