import argparse
import asyncio

import uvloop

from src.utils.enumeration import EntityType

from src.exporters.manager import ExportManager
from src.fetchers.rpc_client import RPCClient
from src.fetchers.raw_receipt import RawReceiptFetcher
from src.clis.utils import get_mapper

from src.parsers.raw_receipt_parser import RawReceiptParser

from src.logger import logger
from src.parsers.event_parser import EventParser
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

    fetcher = RawReceiptFetcher(
        client=client, exporter=exporter[EntityType.RAW_RECEIPT]
    )

    raw_receipt_parser = RawReceiptParser(exporter=exporter, target=entity_types)
    event_parser = EventParser(exporter=exporter)

    for batch_start_block in range(start_block, end_block + 1, process_batch_size):
        batch_end_block = min(batch_start_block + process_batch_size, end_block + 1)

        params = [
            {"block_number": i} for i in range(batch_start_block, batch_end_block)
        ]

        await fetcher.run(
            params=params,
            initial=batch_start_block - start_block,
            total=end_block - start_block + 1,
            batch_size=request_batch_size,
            desc="Raw Receipt: ",
            show_progress=True,
        )

        raw_receipts = [
            raw_receipt["data"] for raw_receipt in exporter[EntityType.RAW_RECEIPT]
        ]
        raw_receipt_parser.parse(
            raw_receipts,
            initial=batch_start_block - start_block,
            total=end_block - start_block + 1,
            batch_size=1,
            show_progress=True,
        )

        event_parser.parse(exporter[EntityType.LOG])

        logger.info(f"Block range: {batch_start_block} - {batch_end_block}")
        logger.info(f"Num RawReceipt: {len(exporter[EntityType.RAW_RECEIPT])}")
        logger.info(f"Num Receipt: {len(exporter[EntityType.RECEIPT])}")
        logger.info(f"Num Log: {len(exporter[EntityType.LOG])}")
        logger.info(f"Num Event: {len(exporter[EntityType.EVENT])}")

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

# python -m src.clis.historical.export_receipts --start-block 23170000 --end-block 23170030 \
#     --process-batch-size 100 --request-batch-size 30 \
# --entity_types raw_receipt,receipt,log --exporter_types duckdb