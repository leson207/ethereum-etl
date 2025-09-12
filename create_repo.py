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
from src.repositories.sqlite.contract import ContractRepository
from src.repositories.sqlite.abi import AbiRepository
from src.repositories.sqlite.pool import PoolRepository
from src.repositories.sqlite.token import TokenRepository
from src.repositories.sqlite.raw_trace import RawTraceRepository
from src.repositories.sqlite.trace import TraceRepository

repos = [
    RawBlockRepository(),
    BlockRepository(),
    TransactionRepository(),
    WithdrawalRepository(),
    RawReceiptRepository(),
    ReceiptRepository(),
    LogRepository(),
    TransferRepository(),
    EventRepository(),
    AccountRepository(),
    ContractRepository(),
    AbiRepository(),
    PoolRepository(),
    TokenRepository(),
    RawTraceRepository(),
    TraceRepository()
]
for repo in repos:
    repo.create(exist_ok=False)