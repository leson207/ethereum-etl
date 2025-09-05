import asyncio

from src.fetchers.raw_block import RawBlockFetcher
from src.fetchers.raw_receipt import RawReceiptFetcher
from src.fetchers.raw_trace import RawTraceFetcher
from src.fetchers.rpc_client import RPCClient
from src.utils.common import dump_json


async def fetch_block(client):
    res = []
    fetcher = RawBlockFetcher(client, exporter=res)
    params = [{"block_number": 23_224_053, "included_transaction": True}]

    await fetcher.run(
        params=params,
        desc="Raw block: ",
        show_progress=True,
    )
    dump_json("raw_block.json", res[0])


async def fetch_receipt(client):
    res = []
    fetcher = RawReceiptFetcher(client, exporter=res)
    params = [{"block_number": 23_224_053}]

    await fetcher.run(
        params=params,
        desc="Raw receipt: ",
        show_progress=True,
    )
    dump_json("raw_receipt.json", res[0])


async def fetch_trace(client):
    res = []
    fetcher = RawTraceFetcher(client, exporter=res)
    params = [{"block_number": 23_224_053}]

    await fetcher.run(
        params=params,
        desc="Raw trace: ",
        show_progress=True,
    )
    dump_json("raw_trace.json", res[0])


async def main():
    client = RPCClient(
        "https://eth-pokt.nodies.app"
    )

    await fetch_block(client)
    await fetch_receipt(client)
    # await fetch_trace(client)


if __name__ == "__main__":
    asyncio.run(main())
