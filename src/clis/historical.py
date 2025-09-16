import argparse
import asyncio

import uvloop

from src.clients.binance_client import BinanceClient
from src.clients.etherscan_client import EtherscanClient
from src.clients.rpc_client import RpcClient
from src.extractors.composite import CompositeExtractor
from src.logger import logger

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


def parse_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-block", type=int)
    parser.add_argument("--end-block", type=int)
    parser.add_argument("--process-batch-size", type=int, default=1000)
    parser.add_argument("--request-batch-size", type=int, default=30)
    parser.add_argument("--entity-types", type=str, default=None)
    parser.add_argument("--exporter-types", type=str, default=None)
    return parser.parse_args()


async def main(
    start_block: int,
    end_block: int,
    process_batch_size: int,
    request_batch_size: int,
    entity_types: list[str],
    exporter_types: list[str],
):
    
    # from src.configs.nats_conn import nats_init
    # await nats_init()

    rpc_client = RpcClient()
    res = await rpc_client.get_web3_client_version()
    logger.info(f"Web3 Client Version: {res[0]['result']}")

    etherscan_client = EtherscanClient()
    binance_client = BinanceClient()

    # entities=[
    #     "raw_block", "block", "transaction", "eth_price",
    #     "raw_receipt", "receipt", "log", "transfer", "event", "account" "contract", "abi", "pool", "token",
    #     "raw_trace", "trace",
    # ]

    extractor = CompositeExtractor(
        entity_types,
        exporter_types,
        rpc_client=rpc_client,
        etherscan_client=etherscan_client,
        binance_client=binance_client,
    )
    await extractor.run(
        start_block=start_block,
        end_block=end_block,
        process_batch_size=process_batch_size,
        request_batch_size=request_batch_size,
    )

    await rpc_client.close()
    await etherscan_client.close()
    await binance_client.close()


if __name__ == "__main__":
    args = parse_arg()
    entity_types = args.entity_types.split(",") if args.entity_types else []
    exporter_types = args.exporter_types.split(",") if args.exporter_types else []
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
#     --process-batch-size 100 --request-batch-size 30 \
#     --entity-types raw_block,block,transaction,withdrawal,raw_receipt,receipt,log,transfer,event,account,contract,abi,pool,token,raw_trace,trace \
#     --exporter-types sqlite
