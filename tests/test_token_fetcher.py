import asyncio

from src.clients.retry import RpcBatchSender
from src.clients.rpc_client import RpcClient
from src.processors.token_fetcher import TokenFetcher


async def main():
    client = RpcClient("https://eth-pokt.nodies.app")
    sender = RpcBatchSender(client)
    fetcher = TokenFetcher(sender)
    await fetcher.run(
        [
            "0xE0f63A424a4439cBE457D80E4f4b51aD25b2c56C"
        ]*100,
        batch_size=5
    )


if __name__ == "__main__":
    asyncio.run(main())
