import asyncio
import uvloop
from sqlalchemy import text
from tabulate import tabulate
from src.configs.duckdb import session
from src.fetchers.block_fetcher import BlockFetcher
from src.fetchers.rpc_client import RPCClient
from src.repositories.duckdb.raw_block import RawBlockRepository
from src.repositories.duckdb.withdrawal import WithdrawalRepository
from src.repositories.duckdb.transaction import TransactionRepository
from src.repositories.duckdb.block import BlockRepository
from src.parsers.block_parser import BlockParser
from src.parsers.transaction_parser import TransactionParser
from src.parsers.withdrawal_parser import WithdrawalParser
from parsers.raw_block_parser import BlockTransactionWithdrawalParser

from src.utils.enumeration import EntityType, ExporterType
from src.exporters.manager import ExporterManager
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def main():
    raw_block_repo = RawBlockRepository()
    block_repo = BlockRepository()
    transaction_repo = TransactionRepository()
    withdrawal_repo = WithdrawalRepository()

    exporter = ExporterManager()
    exporter.add_exporter(EntityType.RAW_BLOCK, ExporterType.DUCKDB, raw_block_repo)
    exporter.add_exporter(EntityType.BLOCK, ExporterType.DUCKDB, block_repo)
    exporter.add_exporter(EntityType.TRANSACTION, ExporterType.DUCKDB, transaction_repo)
    exporter.add_exporter(EntityType.WITHDRAWAL, ExporterType.DUCKDB, withdrawal_repo)

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

    requests = []
    start_block = 23_170_000
    end_block = start_block + 500
    for i in range(start_block, end_block):
        request = {"block_number": i, "included_transaction": True}
        requests.append(request)

    await fetcher.run(
        items=requests,
        initial=0,
        total=end_block-start_block+1,
        batch_size=30,
        show_progress=True,
    )
    # exporter.export(EntityType.RAW_BLOCK)
    block_transaction_withdrawal_parser.parse([raw_block['data'] for raw_block in exporter.get_item(EntityType.RAW_BLOCK)])
    
    print(exporter.data.keys())
    print(len(exporter.get_item(EntityType.RAW_BLOCK)))
    print(len(exporter.get_item(EntityType.BLOCK)))
    print(len(exporter.get_item(EntityType.TRANSACTION)))
    print(len(exporter.get_item(EntityType.WITHDRAWAL)))

    await client.close()
    exporter.clear_all()

def test():
    repo_type = TransactionRepository
    repo = repo_type()
    repo.create(exist_ok=False)
    repo.inspect()

def query_database():
    query = "SELECT COUNT() FROM block"
    query = text(query)
    res = session.execute(query)
    print(tabulate(res.fetchall(), headers=res.keys(), tablefmt="pretty"))

if __name__ == "__main__":
    # asyncio.run(main())
    test()
    # query_database()