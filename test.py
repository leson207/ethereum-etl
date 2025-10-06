from src.services.pool_service import PoolService
from src.clients.rpc_client import RpcClient
import asyncio


async def main():
    client = RpcClient()
    service = PoolService(client)
    address = "0x8eea6cc08d824b20efb3bf7c248de694cb1f75f4"
    # address = "0xbcca60bb61934080951369a648fb03df4f96263c"
    # address = "0x00a0be1bbc0c99898df7e6524bf16e893c1e3bb9"
    print("8"*100)
    token0 = await service.get_token0_address(address)
    print(token0)


if __name__ == "__main__":
    asyncio.run(main())
