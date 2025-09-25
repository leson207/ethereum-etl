from src.utils.enumeration import Entity


async def export_raw_block(results, **kwargs):
    from src.repositories.sqlite.raw_block import RawBlockRepository

    repo = RawBlockRepository()
    await repo.insert(results[Entity.RAW_BLOCK], deduplicate="replace")


async def export_block(results, **kwargs):
    from src.repositories.sqlite.block import BlockRepository

    repo = BlockRepository()
    await repo.insert(results[Entity.BLOCK], deduplicate="replace")


async def export_transaction(results, **kwargs):
    from src.repositories.sqlite.transaction import TransactionRepository

    repo = TransactionRepository()
    await repo.insert(results[Entity.TRANSACTION], deduplicate="replace")


async def export_withdrawal(results, **kwargs):
    from src.repositories.sqlite.withdrawal import WithdrawalRepository

    repo = WithdrawalRepository()
    await repo.insert(results[Entity.WITHDRAWAL], deduplicate="replace")


entity_task = {
    Entity.RAW_BLOCK: export_raw_block,
    Entity.BLOCK: export_block,
    Entity.TRANSACTION: export_transaction,
    Entity.WITHDRAWAL: export_withdrawal,
}
