import argparse
import asyncio

import uvloop

from src.clis.utils import get_mapper
from src.exporters.manager import ExportManager
from src.fetchers.raw_block import RawBlockFetcher
from src.fetchers.raw_receipt import RawReceiptFetcher
from src.fetchers.rpc_client import RPCClient
from src.logger import logger
from src.parsers.enrich import enrich_events
from src.parsers.event_parser import EventParser
from src.parsers.raw_block_parser import RawBlockParser
from src.parsers.raw_receipt_parser import RawReceiptParser
from src.utils.enumeration import EntityType

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# TODO: redesign this file
def parse_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-block", type=int)
    parser.add_argument("--end-block", type=int)
    parser.add_argument("--process-batch-size", type=int, default=1000)
    parser.add_argument("--request-batch-size", type=int, default=30)
    parser.add_argument("--entity_types", type=str, default="")
    parser.add_argument("--exporter_types", type=str, default="")
    return parser.parse_args()


async def extract_block(
    exporter,
    fetcher,
    parser,
    start_block,
    end_block,
    batch_start_block,
    batch_end_block,
    request_batch_size,
):
    params = [
        {"block_number": i, "included_transaction": True}
        for i in range(batch_start_block, batch_end_block)
    ]

    await fetcher.run(
        params=params,
        initial=batch_start_block - start_block,
        total=end_block - start_block + 1,
        batch_size=request_batch_size,
        desc="Raw Block: ",
        show_progress=True,
    )

    raw_blocks = [raw_block["data"] for raw_block in exporter[EntityType.RAW_BLOCK]]
    parser.parse(
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

async def extract_receipt(
    exporter,
    fetcher,
    parser,
    start_block,
    end_block,
    batch_start_block,
    batch_end_block,
    request_batch_size,
):
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
    parser.parse(
        raw_receipts,
        initial=batch_start_block - start_block,
        total=end_block - start_block + 1,
        batch_size=1,
        show_progress=True,
    )

    logger.info(f"Block range: {batch_start_block} - {batch_end_block}")
    logger.info(f"Num RawReceipt: {len(exporter[EntityType.RAW_RECEIPT])}")
    logger.info(f"Num Receipt: {len(exporter[EntityType.RECEIPT])}")
    logger.info(f"Num Log: {len(exporter[EntityType.LOG])}")

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

    client = RPCClient(uri="https://eth-pokt.nodies.app")
    res = await client.send_batch_request([("web3_clientVersion", [])])
    logger.info(f"Web3 Client Version: {res[0]['result']}")

    raw_block_fetcher = RawBlockFetcher(
        client=client, exporter=exporter[EntityType.RAW_BLOCK]
    )
    raw_block_parser = RawBlockParser(exporter=exporter, target=entity_types)

    raw_receipt_fetcher = RawReceiptFetcher(
        client=client, exporter=exporter[EntityType.RAW_RECEIPT]
    )
    raw_receipt_parser = RawReceiptParser(exporter=exporter, target=entity_types)
    event_parser = EventParser(exporter=exporter)

    for batch_start_block in range(start_block, end_block + 1, process_batch_size):
        batch_end_block = min(batch_start_block + process_batch_size, end_block + 1)

        if not set(entity_types).isdisjoint([EntityType.RAW_BLOCK,EntityType.BLOCK,EntityType.TRANSACTION, EntityType.WITHDRAWAL]):
            await extract_block(
                exporter,
                raw_block_fetcher,
                raw_block_parser,
                start_block,
                end_block,
                batch_start_block,
                batch_end_block,
                request_batch_size,
            )
        if not set(entity_types).isdisjoint([EntityType.RAW_RECEIPT,EntityType.RECEIPT,EntityType.LOG]):
            await extract_receipt(
                exporter,
                raw_receipt_fetcher,
                raw_receipt_parser,
                start_block,
                end_block,
                batch_start_block,
                batch_end_block,
                request_batch_size,
            )

        event_parser.parse(exporter[EntityType.LOG])
        logger.info(f"Num Event: {len(exporter[EntityType.EVENT])}")

        enrich_events(exporter[EntityType.EVENT], exporter[EntityType.BLOCK], exporter[EntityType.TRANSACTION])

        exporter.export_all()
        exporter.clear_all()

    await client.close()


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

# python -m test --start-block 23170000 --end-block 23170030 \
#     --process-batch-size 100 --request-batch-size 30 \
# --entity_types block,transaction,log
