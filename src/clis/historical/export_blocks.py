import argparse
import asyncio

import uvloop

from src.clis.utils import get_mapper
from src.exporters.manager import ExportManager
from src.fetchers.raw_block import RawBlockFetcher
from src.fetchers.rpc_client import RPCClient
from src.logger import logger
from src.parsers.block_parser import BlockParser
from src.parsers.transaction_parser import TransactionParser
from src.parsers.withdrawal_parser import WithdrawalParser
from src.utils.enumeration import EntityType

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


async def main(start_block, end_block, process_batch_size, request_batch_size, entity_types, exporter_types):
    mapper = get_mapper(entity_types, exporter_types)
    exporter = ExportManager(mapper)

    client = RPCClient(uri="https://eth-pokt.nodies.app")
    res = await client.send_batch_request([("web3_clientVersion", [])])
    logger.info(f"Web3 Client Version: {res[0]['result']}")

    fetcher = RawBlockFetcher(client=client, exporter=exporter)

    block_parser = BlockParser(exporter=exporter)
    transaction_parser = TransactionParser(exporter=exporter)
    withdrawal_parser = WithdrawalParser(exporter=exporter)

    for batch_start_block in range(start_block, end_block + 1, process_batch_size):
        batch_end_block = min(batch_start_block + process_batch_size, end_block + 1)

        params = [
            {"block_number": i, "included_transaction": True}
            for i in range(batch_start_block, batch_end_block)
        ]

        await fetcher.run(
            params=params,
            initial=batch_start_block - start_block,
            total=end_block - start_block + 1,
            batch_size=request_batch_size,
            show_progress=True,
        )

        raw_blocks = [
            raw_block["data"] for raw_block in exporter[EntityType.RAW_BLOCK]
        ]
        block_parser.parse(
            raw_blocks,
            initial=batch_start_block - start_block,
            total=end_block - start_block + 1,
            batch_size=1,
            show_progress=True,
        )
        transaction_parser.parse(
            raw_blocks,
            initial=batch_start_block - start_block,
            total=end_block - start_block + 1,
            batch_size=1,
            show_progress=True,
        )
        withdrawal_parser.parse(
            raw_blocks,
            initial=batch_start_block - start_block,
            total=end_block - start_block + 1,
            batch_size=1,
            show_progress=True,
        )

        logger.info(f"Block range: {batch_start_block} - {batch_end_block}")
        logger.info(f"Num RawBlock: {len(exporter[EntityType.RAW_BLOCK])}")
        logger.info(f"Num Block: {len(exporter[EntityType.BLOCK])}")
        logger.info(f"Num Transaction: {len(exporter[EntityType.TRANSACTION])}")
        logger.info(f"Num Withdrawal: {len(exporter[EntityType.WITHDRAWAL])}")

        exporter.export_all()
        exporter.clear_all()

    await client.close()


if __name__ == "__main__":
    args = parse_arg()
    entity_types = args.entity_types.split(',')
    exporter_types = args.exporter_types.split(',')
    asyncio.run(
        main(
            args.start_block,
            args.end_block,
            args.process_batch_size,
            args.request_batch_size,
            entity_types,
            exporter_types
        )
    )

# python -m src.clis.historical.export_blocks --start-block 23170000 --end-block 23170030 \
#     --process-batch-size 100 --request-batch-size 30 \
# --entity_types raw_block,block,transaction,withdrawal --exporter_types sqlite
