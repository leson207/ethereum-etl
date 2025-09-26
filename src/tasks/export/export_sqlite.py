from src.utils.enumeration import Entity

# sqlite not thread safe so mark this function as async to make sure it run on the same thread when execute dag


async def export_raw_block(results, **kwargs):
    from src.repositories.sqlite.raw_block import RawBlockRepository

    repo = RawBlockRepository()
    repo.insert(results[Entity.RAW_BLOCK], deduplicate="replace")


async def export_block(results, **kwargs):
    from src.repositories.sqlite.block import BlockRepository

    repo = BlockRepository()
    repo.insert(results[Entity.BLOCK], deduplicate="replace")


async def export_transaction(results, **kwargs):
    from src.repositories.sqlite.transaction import TransactionRepository

    repo = TransactionRepository()
    repo.insert(results[Entity.TRANSACTION], deduplicate="replace")


async def export_withdrawal(results, **kwargs):
    from src.repositories.sqlite.withdrawal import WithdrawalRepository

    repo = WithdrawalRepository()
    repo.insert(results[Entity.WITHDRAWAL], deduplicate="replace")


entity_func = {
    Entity.RAW_BLOCK: export_raw_block,
    Entity.BLOCK: export_block,
    Entity.TRANSACTION: export_transaction,
    Entity.WITHDRAWAL: export_withdrawal,
}
