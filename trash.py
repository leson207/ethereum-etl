from src.services.pool_service import PoolService
from src.clients.rpc_client import RpcClient
from src.configs.environment import env
import asyncio
from src.abis.function import FUNCTION_HEX_SIGNATURES

client = RpcClient(uris=env.PROVIDER_URIS)
service = PoolService(client)

# 0x70a08231b98ef4ca268c9cc3f6b4590e4bfec28280db06bb5d45e689f2a360be
def encode_balance_of_call(pool_address: str) -> str:
    selector = "0x70a08231"  # keccak("balanceOf(address)")[:4]
    addr = pool_address.lower().replace("0x", "").rjust(64, "0")
    return selector + addr

print(FUNCTION_HEX_SIGNATURES["erc20"]["balanceOf"])

res = asyncio.run(service.get_token_balance(
    pool_address=encode_balance_of_call("0xc3Db44ADC1fCdFd5671f555236eae49f4A8EEa18"),
    token_address="0x57e114B691Db790C35207b2e685D4A43181e6061"
))
print(res)