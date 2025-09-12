import argparse
import asyncio
import time

import httpx
import orjson
import uvloop
from httpx_ws import aconnect_ws

from src.clients.binance_client import BinanceClient
from src.clients.etherscan_client import EtherscanClient
from src.clients.rpc_client import RpcClient
from src.configs.environment import env
from src.extractors.composite import CompositeExtractor
from src.logger import logger

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


def parse_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entity-types", type=str, default="")
    parser.add_argument("--exporter-types", type=str, default="")
    return parser.parse_args()


async def main(entity_types, exporter_types):
    ws_url = "wss://mainnet.infura.io/ws/v3/29b89f1d2c8347d291a17088b2bf2a52"

    rpc_client = RpcClient(env.PROVIDER_URIS)
    res = await rpc_client.get_web3_client_version()
    logger.info(f"Web3 Client Version: {res[0]['result']}")

    etherscan_client = EtherscanClient(url="https://api.etherscan.io/v2/api")
    binance_client = BinanceClient(url="https://api4.binance.com/api/v3")

    extractor = CompositeExtractor(
        entity_types,
        exporter_types,
        rpc_client=rpc_client,
        etherscan_client=etherscan_client,
        binance_client=binance_client,
    )

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

                await extractor.run(
                    start_block=block_number,
                    end_block=block_number,
                    process_batch_size=30,
                    request_batch_size=30,
                )
                end = time.perf_counter()
                logger.info(
                    f"Done process block number: {block_number} - Elapsed: {end - start:.6f} seconds"
                )
        # except Exception as e:
        #     logger.info(e)
        finally:
            await ws.send_json(
                {
                    "id": 1,
                    "jsonrpc": "2.0",
                    "method": "eth_unsubscribe",
                    "params": [subscription_id],
                }
            )

            await rpc_client.close()
            await etherscan_client.close()
            await binance_client.close()


if __name__ == "__main__":
    args = parse_arg()
    entity_types = args.entity_types.split(",")
    exporter_types = args.exporter_types.split(",")
    asyncio.run(main(entity_types, exporter_types))


# python -m src.clis.realtime_ws \
#     --entity-types raw_block,block,transaction,withdrawal,raw_receipt,receipt,log,transfer,event,account,contract,abi,pool,token,raw_trace,trace \
#     --exporter-types sqlite
