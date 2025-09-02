from src.parsers.block_parser import BlockParser
from src.parsers.transaction_parser import TransactionParser
from src.parsers.withdrawal_parser import WithdrawalParser
from rich.progress import Progress


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
        with Progress(disable=not show_progress) as progress:
            task = progress.add_task(
                "Block - Transaction - Withdrawal: ",
                total=(total or len(items)),
                completed=(initial or 0),
            )

            for item in items:
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
                progress.update(task, advance=batch_size)