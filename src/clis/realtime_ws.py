import argparse
import asyncio
import time

import httpx
import orjson
from httpx_ws import aconnect_ws

from exporters.utils import get_mapper
from src.exporters.manager import ExportManager
from src.fetchers.raw_block import RawBlockFetcher
from src.fetchers.rpc_client import RPCClient
from src.logger import logger
from src.parsers.raw_block_parser import RawBlockParser
from src.utils.enumeration import EntityType

# TODO: higher level for uri


def parse_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entity_types", type=str, default="")
    parser.add_argument("--exporter_types", type=str, default="")
    return parser.parse_args()


async def main(entity_types, exporter_types):
    # url = "https://mainnet.infura.io/v3/29b89f1d2c8347d291a17088b2bf2a52"
    url = "https://eth-pokt.nodies.app"
    ws_url = "wss://mainnet.infura.io/ws/v3/29b89f1d2c8347d291a17088b2bf2a52"

    mapper = get_mapper(entity_types, exporter_types)
    exporter = ExportManager(mapper)

    rpc_client = RPCClient(uri=url)
    res = await rpc_client.send_batch_request([("web3_clientVersion", [])])
    logger.info(f"Web3 Client Version: {res[0]['result']}")

    raw_block_fetcher = RawBlockFetcher(
        client=rpc_client,
        exporter=exporter[EntityType.RAW_BLOCK],
        max_retries=100,
        backoff=1,
    )
    raw_block_parser = RawBlockParser(exporter=exporter, target=entity_types)

    ws_client = httpx.AsyncClient()

    async with aconnect_ws(ws_url, ws_client) as ws:
        await ws.send_json(
            {
                "id": 1,
                "jsonrpc": "2.0",
                "method": "eth_subscribe",
                "params": ["newHeads"],
            }
        )
        msg = await ws.receive_text()
        msg = orjson.loads(msg)
        subscription_id = msg["result"]
        logger.info(f"Subscription ID: {subscription_id}")
        try:
            while True:
                message = await ws.receive_text()
                start = time.perf_counter()
                message = orjson.loads(message)
                block_number = int(message["params"]["result"]["number"], 16)
                logger.info(f"Start process block number: {block_number}")

                block_params = [
                    {"block_number": block_number, "included_transaction": True}
                ]

                await asyncio.gather(
                    raw_block_fetcher.run(
                        params=block_params,
                        initial=0,
                        total=1,
                        batch_size=30,
                        desc="Raw Block: ",
                        show_progress=False,
                    ),
                )
                raw_blocks = [
                    raw_block["data"] for raw_block in exporter[EntityType.RAW_BLOCK]
                ]
                raw_block_parser.parse(
                    raw_blocks,
                    initial=0,
                    total=1,
                    batch_size=1,
                    show_progress=True,
                )

                exporter.export_all()
                exporter.clear_all()
                end = time.perf_counter()
                logger.info(
                    f"Done process block number: {block_number} - Elapsed: {end - start:.6f} seconds"
                )
        finally:
            await ws.send_json(
                {
                    "id": 1,
                    "jsonrpc": "2.0",
                    "method": "eth_unsubscribe",
                    "params": [subscription_id],
                }
            )


if __name__ == "__main__":
    args = parse_arg()
    entity_types = args.entity_types.split(",")
    exporter_types = args.exporter_types.split(",")
    asyncio.run(main(entity_types, exporter_types))

# python -m src.clis.real_time.ws_block --entity_types raw_block,block,transaction,withdrawal --exporter_types duckdb
