import asyncio

from src.clients.retry import RpcBatchSender
from src.clients.rpc_client import RpcClient
from src.processors.raw_block_fetcher import RawBlockFetcher
from src.exporters.manager import ExportManager
from src.exporters.mapper import EntityExporterMapper

async def main():
    mapper = EntityExporterMapper()
    exporter = ExportManager(mapper)

    client = RpcClient("https://eth-pokt.nodies.app")
    sender = RpcBatchSender(client)
    
    fetcher = RawBlockFetcher(sender,exporter)
    await fetcher.run(range(0, 100))


if __name__ == "__main__":
    asyncio.run(main())
