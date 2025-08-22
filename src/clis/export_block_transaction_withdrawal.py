import argparse
import asyncio

import uvloop

from src.exporters.manager import ExporterManager
from src.fetchers.block_fetcher import BlockFetcher
from src.fetchers.rpc_client import RPCClient
from src.parsers.block_parser import BlockParser
from src.parsers.block_transaction_withdrawal_parser import (
    BlockTransactionWithdrawalParser,
)
from src.parsers.transaction_parser import TransactionParser
from src.parsers.withdrawal_parser import WithdrawalParser
from src.utils.entity_exporter_mapping import EntityExporterMapper
from src.utils.enumeration import EntityType

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


def parse_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-block", type=int)
    parser.add_argument("--end-block", type=int)
    parser.add_argument("--process-batch-size", type=int, default=1000)
    parser.add_argument("--request-batch-size", type=int, default=30)
    parser.add_argument("--entity-exporter", type=str)
    return parser.parse_args()


async def main(
    start_block, end_block, process_batch_size, request_batch_size, entity_exporter
):
    mapper = create_entity_exporter_mapper(entity_exporter)
    exporter = ExporterManager(mapper)

    client = RPCClient(uri="https://eth-pokt.nodies.app")
    fetcher = BlockFetcher(client=client, exporter=exporter)

    block_parser = BlockParser(exporter=exporter)
    transaction_parser = TransactionParser(exporter)
    withdrawal_parser = WithdrawalParser(exporter)

    block_transaction_withdrawal_parser = BlockTransactionWithdrawalParser(
        block_parser=block_parser,
        transaction_parser=transaction_parser,
        withdrawal_parser=withdrawal_parser,
    )

    for block_number in range(start_block, end_block + 1, process_batch_size):
        initial = block_number - start_block
        total = end_block - start_block + 1

        requests = [
            {"block_number": i, "included_transaction": True}
            for i in range(
                block_number, min(block_number + process_batch_size, end_block+1)
            )
        ]

        await fetcher.run(
            items=requests,
            initial=initial,
            total=total,
            batch_size=request_batch_size,
            show_progress=True,
        )

        block_transaction_withdrawal_parser.parse(
            [
                raw_block["data"]
                for raw_block in exporter.get_item(EntityType.RAW_BLOCK)
            ],
            initial=initial,
            total=total,
            batch_size=1,
            show_progress=True,
        )

        # exporter.export_all()
        exporter.clear_all()

    await client.close()


def create_entity_exporter_mapper(s):
    mapper = EntityExporterMapper()

    entity_exporter = s.split(",")
    for entity_exporter in s.split(","):
        entity, exporter = entity_exporter.split("|")
        mapper.set(entity, exporter)

    return mapper


if __name__ == "__main__":
    args = parse_arg()
    asyncio.run(
        main(
            args.start_block,
            args.end_block,
            args.process_batch_size,
            args.request_batch_size,
            args.entity_exporter,
        )
    )

# python -m src.clis.export_blocks_and_transactions --start-block 23170000 --end-block 23170500 \
#     --process-batch-size 100 --request-batch-size 30 \
#     --entity-exporter "raw_block|duckdb,block|duckdb,transaction|duckdb,withdrawal|duckdb"
