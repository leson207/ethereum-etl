from src.utils.enumeration import Entity

# sqlite not thread safe so mark this function as async to make sure it run on the same thread when execute dag


async def export_raw_block(results: dict[str, list], **kwargs):
    from src.repositories.sqlite.raw_block import RawBlockRepository

    repo = RawBlockRepository()
    repo.insert(results[Entity.RAW_BLOCK], deduplicate="replace")

async def export_block(results: dict[str, list], **kwargs):
    from src.repositories.sqlite.block import BlockRepository

    repo = BlockRepository()
    repo.insert(results[Entity.BLOCK], deduplicate="replace")

async def export_transaction(results: dict[str, list], **kwargs):
    from src.repositories.sqlite.transaction import TransactionRepository

    repo = TransactionRepository()
    repo.insert(results[Entity.TRANSACTION], deduplicate="replace")

async def export_withdrawal(results: dict[str, list], **kwargs):
    from src.repositories.sqlite.withdrawal import WithdrawalRepository

    repo = WithdrawalRepository()
    repo.insert(results[Entity.WITHDRAWAL], deduplicate="replace")

async def export_raw_receipt(results: dict[str, list], **kwargs):
    from src.repositories.sqlite.raw_receipt import RawReceiptRepository

    repo = RawReceiptRepository()
    repo.insert(results[Entity.RAW_RECEIPT], deduplicate="replace")

async def export_receipt(results: dict[str, list], **kwargs):
    from src.repositories.sqlite.receipt import ReceiptRepository

    repo = ReceiptRepository()
    repo.insert(results[Entity.RECEIPT], deduplicate="replace")

async def export_log(results: dict[str, list], **kwargs):
    from src.repositories.sqlite.log import LogRepository

    repo = LogRepository()
    repo.insert(results[Entity.LOG], deduplicate="replace")

async def export_transfer(results: dict[str, list], **kwargs):
    from src.repositories.sqlite.transfer import TransferRepository

    repo = TransferRepository()
    repo.insert(results[Entity.TRANSFER], deduplicate="replace")

async def export_event(results: dict[str, list], **kwargs):
    from src.repositories.sqlite.event import EventRepository

    repo = EventRepository()
    repo.insert(results[Entity.EVENT], deduplicate="replace")

async def export_account(results: dict[str, list], **kwargs):
    from src.repositories.sqlite.account import AccountRepository

    repo = AccountRepository()
    repo.insert(results[Entity.ACCOUNT], deduplicate="replace")

async def export_pool(results: dict[str, list], **kwargs):
    from src.repositories.sqlite.pool import PoolRepository

    repo = PoolRepository()
    repo.insert(results[Entity.POOL], deduplicate="replace")

async def export_token(results: dict[str, list], **kwargs):
    from src.repositories.sqlite.token import TokenRepository

    repo = TokenRepository()
    repo.insert(results[Entity.TOKEN], deduplicate="replace")

async def export_raw_trace(results: dict[str, list], **kwargs):
    from src.repositories.sqlite.raw_trace import RawTraceRepository

    repo = RawTraceRepository()
    repo.insert(results[Entity.RAW_TRACE], deduplicate="replace")

async def export_trace(results: dict[str, list], **kwargs):
    from src.repositories.sqlite.trace import TraceRepository

    repo = TraceRepository()
    repo.insert(results[Entity.TRACE], deduplicate="replace")

entity_func = {
    Entity.RAW_BLOCK: export_raw_block,
    Entity.BLOCK: export_block,
    Entity.TRANSACTION: export_transaction,
    Entity.WITHDRAWAL: export_withdrawal,
    
    Entity.RAW_RECEIPT: export_raw_receipt,
    Entity.RECEIPT: export_receipt,
    Entity.LOG: export_log,
    Entity.TRANSFER: export_transfer,
    Entity.EVENT: export_event,
    Entity.ACCOUNT: export_account,
    Entity.POOL: export_pool,
    Entity.TOKEN: export_token,

    Entity.RAW_TRACE: export_raw_trace,
    Entity.TRACE: export_trace
}
