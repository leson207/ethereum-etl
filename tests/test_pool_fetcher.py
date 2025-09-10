import asyncio

from src.clients.retry import RpcBatchSender
from src.clients.rpc_client import RpcClient
from src.processors.pool_fetcher import PoolFetcher


async def main():
    client = RpcClient("https://eth-pokt.nodies.app")
    sender = RpcBatchSender(client)
    fetcher = PoolFetcher(sender)
    await fetcher.run(
        [
            "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
        ]*100,
        batch_size=14
    )


if __name__ == "__main__":
    asyncio.run(main())
