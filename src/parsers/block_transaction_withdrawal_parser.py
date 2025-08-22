from tqdm.std import tqdm

from src.parsers.block_parser import BlockParser
from src.parsers.transaction_parser import TransactionParser
from src.parsers.withdrawal_parser import WithdrawalParser
from src.utils.progress_bar import get_progress_bar


class BlockTransactionWithdrawalParser:
    def __init__(
        self,
        block_parser: BlockParser,
        transaction_parser: TransactionParser,
        withdrawal_parser: WithdrawalParser,
    ):
        self.block_parser = block_parser
        self.transaction_parser = transaction_parser
        self.withdrawal_parser = withdrawal_parser

    def parse(
        self,
        items: list[dict],
        initial=None,
        total=None,
        batch_size=1,
        show_progress=True,
    ):
        p_bar = get_progress_bar(
            tqdm,
            items,
            initial=(initial or 0) // batch_size,
            total=(total or len(items)) // batch_size,
            show=show_progress,
        )
        for item in p_bar:
            self.block_parser.parse([item], show_progress=False)
            self.transaction_parser.parse(
                item.get("transactions", []), show_progress=False
            )
            self.withdrawal_parser.parse(
                item.get("withdrawals", []),
                block_hash=item["hash"],
                block_number=item["number"],
                show_progress=False,
            )
