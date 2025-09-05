import asyncio
import uvloop
from sqlalchemy import text
from tabulate import tabulate
from src.configs.duckdb import session
from src.fetchers.block_fetcher import BlockFetcher
from src.fetchers.rpc_client import RPCClient

import orjson
from src.parsers.block_parser import BlockParser
from src.parsers.transaction_parser import TransactionParser
from src.parsers.withdrawal_parser import WithdrawalParser
from parsers.raw_block_parser import BlockTransactionWithdrawalParser
from parsers.raw_receipt_parser import ReceiptLogParser
from parsers.address_parser import LogParser
from src.utils.entity_exporter_mapping import EntityExporterMapper
from src.utils.enumeration import EntityType, ExporterType
from src.fetchers.receipt_fetcher import ReceiptFetcher
from src.exporters.manager import ExporterManager
from src.fetchers.throttler import Throttler
from parsers.transfer_parser import ReceiptParser
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

async def phase_one(client, exporter, start_block, end_block):
    block_fetcher = BlockFetcher(client=client, exporter=exporter)

    block_parser = BlockParser(exporter=exporter)
    transaction_parser = TransactionParser(exporter)
    withdrawal_parser = WithdrawalParser(exporter)

    block_transaction_withdrawal_parser = BlockTransactionWithdrawalParser(
        block_parser=block_parser,
        transaction_parser=transaction_parser,
        withdrawal_parser=withdrawal_parser,
    )

    requests = [
        {"block_number": i, "included_transaction": True}
        for i in range(start_block, end_block)
    ]

    await block_fetcher.run(
        items=requests,
        initial=0,
        total=len(requests),
        batch_size=30,
        show_progress=True,
    )
    block_transaction_withdrawal_parser.parse([raw_block['data'] for raw_block in exporter.get_item(EntityType.RAW_BLOCK)])
    
    print(exporter.data.keys())
    print(len(exporter.get_item(EntityType.RAW_BLOCK)))
    print(len(exporter.get_item(EntityType.BLOCK)))
    print(len(exporter.get_item(EntityType.TRANSACTION)))
    print(len(exporter.get_item(EntityType.WITHDRAWAL)))

async def phase_two(client, exporter):
    client.throttler = Throttler(rate_limit=100, period=1)
    receipt_fetcher = ReceiptFetcher(client=client, exporter=exporter)

    receipt_parser = ReceiptParser(exporter)
    log_parser = LogParser(exporter)
    receipt_log_parser = ReceiptLogParser(receipt_parser=receipt_parser, log_parser = log_parser)

    requests = [
        {"transaction_hash": i["transaction_hash"],}
        for i in exporter.data[EntityType.TRANSACTION]
    ]
    await receipt_fetcher.run(
        items=requests,
        initial=0,
        total=len(requests),
        batch_size=30,
        show_progress=True
    )
    
    # tmp = exporter.get_item(EntityType.RAW_RECEIPT)[0]
    # with open("draft.json", "wb") as f:
    #     f.write(orjson.dumps(tmp, option=orjson.OPT_INDENT_2))

    # receipt_parser.parse([raw_receipt['data'] for raw_receipt in exporter.get_item(EntityType.RAW_RECEIPT)])

    # for raw_receipt in exporter.get_item(EntityType.RAW_RECEIPT):
    #     log_parser.parse(raw_receipt['data'].get("logs"))

    receipt_log_parser.parse([raw_receipt['data'] for raw_receipt in exporter.get_item(EntityType.RAW_RECEIPT)])
    print(len(exporter.get_item(EntityType.RECEIPT)))
    print(len(exporter.get_item(EntityType.LOG)))

async def main():
    # Config
    mapper = EntityExporterMapper()
    # mapper.set(EntityType.RAW_BLOCK, ExporterType.DUCKDB)
    # mapper.set(EntityType.BLOCK, ExporterType.DUCKDB)
    # mapper.set(EntityType.TRANSACTION, ExporterType.DUCKDB)
    # mapper.set(EntityType.WITHDRAWAL, ExporterType.DUCKDB)
    # mapper.set(EntityType.RAW_RECEIPT, ExporterType.DUCKDB)
    # mapper.set(EntityType.RECEIPT, ExporterType.DUCKDB)
    exporter = ExporterManager(mapper)

    client = RPCClient(uri="https://eth-pokt.nodies.app")

    start_block = 23_170_000
    end_block = start_block + 10

    await phase_one(client, exporter, start_block, end_block)
    await phase_two(client, exporter)

    await client.close()
    # exporter.export(EntityType.RAW_RECEIPT)
    exporter.clear_all()

def test():
    from src.repositories.duckdb.raw_block import RawBlockRepository
    from src.repositories.duckdb.block import BlockRepository
    from src.repositories.duckdb.transaction import TransactionRepository
    from src.repositories.duckdb.withdrawal import WithdrawalRepository
    from src.repositories.duckdb.raw_receipt import RawReceiptRepository

    repos = [
        RawBlockRepository(),
        BlockRepository(),
        TransactionRepository(),
        WithdrawalRepository(),

        RawReceiptRepository()
    ]
    for repo in repos:
        repo.create(exist_ok=False)
        repo.inspect()

def query_database():
    query = "SELECT COUNT() FROM block"
    query = text(query)
    res = session.execute(query)
    print(tabulate(res.fetchall(), headers=res.keys(), tablefmt="pretty"))


if __name__ == "__main__":
    asyncio.run(main())
    # test()
    # query_database()