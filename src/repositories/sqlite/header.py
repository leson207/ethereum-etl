from src.repositories.sqlite.raw_block import RawBlockRepository
from src.repositories.sqlite.block import BlockRepository
from src.repositories.sqlite.transaction import TransactionRepository
from src.repositories.sqlite.withdrawal import WithdrawalRepository
from src.repositories.sqlite.raw_receipt import RawReceiptRepository
from src.repositories.sqlite.receipt import ReceiptRepository
from src.repositories.sqlite.log import LogRepository
from src.repositories.sqlite.transfer import TransferRepository
from src.repositories.sqlite.event import EventRepository
from src.repositories.sqlite.account import AccountRepository
from src.repositories.sqlite.pool import PoolRepository
from src.repositories.sqlite.token import TokenRepository
from src.repositories.sqlite.raw_trace import RawTraceRepository
from src.repositories.sqlite.trace import TraceRepository
from src.utils.enumeration import Entity

repo_dict={
    Entity.RAW_BLOCK: RawBlockRepository,
    Entity.BLOCK: BlockRepository,
    Entity.TRANSACTION: TransactionRepository,
    Entity.WITHDRAWAL: WithdrawalRepository,

    Entity.RAW_RECEIPT: RawReceiptRepository,
    Entity.RECEIPT: ReceiptRepository,
    Entity.LOG: LogRepository,
    Entity.ACCOUNT: AccountRepository,
    Entity.TRANSFER: TransferRepository,
    Entity.EVENT: EventRepository,
    Entity.POOL: PoolRepository,
    Entity.TOKEN: TokenRepository,

    Entity.RAW_TRACE: RawTraceRepository,
    Entity.TRACE: TraceRepository
}
