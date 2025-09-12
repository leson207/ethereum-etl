import argparse
import asyncio

import uvloop

from src.clients.etherscan_client import EtherscanClient
from src.clients.rpc_client import RpcClient
from src.exporters.utils import get_mapper
from src.exporters.manager import ExportManager
from src.extractors.composite import CompositeExtractor
from src.logger import logger
from src.clients.binance_client import BinanceClient
from src.configs.environment import env
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


def parse_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-block", type=int)
    parser.add_argument("--end-block", type=int)
    parser.add_argument("--process-batch-size", type=int, default=1000)
    parser.add_argument("--request-batch-size", type=int, default=30)
    parser.add_argument("--entity_types", type=str, default="")
    parser.add_argument("--exporter_types", type=str, default="")
    return parser.parse_args()


async def main(
    start_block,
    end_block,
    process_batch_size,
    request_batch_size,
    entity_types,
    exporter_types,
):
    mapper = get_mapper(entity_types, exporter_types)
    exporter = ExportManager(mapper)

    rpc_client = RpcClient(env.PROVIER_URIS)
    res = await rpc_client.get_web3_client_version()
    logger.info(f"Web3 Client Version: {res[0]['result']}")

    etherscan_client = EtherscanClient(url="https://api.etherscan.io/v2/api")
    binance_client = BinanceClient(url="https://api4.binance.com/api/v3")

    extractor = CompositeExtractor(
        exporter=exporter, rpc_client=rpc_client, etherscan_client=etherscan_client, binance_client = binance_client
    )
    await extractor.run(
        start_block=start_block,
        end_block=end_block,
        process_batch_size=process_batch_size,
        request_batch_size=request_batch_size,
        entities=[
            "raw_block", "block", "transaction", "eth_price",
            "raw_receipt", "receipt", "log", "transfer", "account", "event", "contract", "abi", "pool", "token",
            "raw_trace", "trace",
        ],
    )

    await rpc_client.close()
    await etherscan_client.close()
    await binance_client.close()


if __name__ == "__main__":
    args = parse_arg()
    entity_types = args.entity_types.split(",")
    exporter_types = args.exporter_types.split(",")
    asyncio.run(
        main(
            args.start_block,
            args.end_block,
            args.process_batch_size,
            args.request_batch_size,
            entity_types,
            exporter_types,
        )
    )

# python -m src.clis.historical --start-block 23170000 --end-block 23170030 \
#     --process-batch-size 100 --request-batch-size 30