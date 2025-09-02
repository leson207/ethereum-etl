from rich.progress import Progress

from src.parsers.log_parser import LogParser
from src.parsers.receipt_parser import ReceiptParser


class ReceiptLogParser:
    def __init__(
        self,
        receipt_parser: ReceiptParser,
        log_parser: LogParser,
    ):
        self.receipt_parser = receipt_parser
        self.log_parser = log_parser

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
                "Receipt - Log: ",
                total=(total or len(items)),
                completed=(initial or 0),
            )

            for item in items:
                self.receipt_parser.parse(item, show_progress=False)
                logs = []
                for i in item:
                    logs.extend(i.get("logs", []))
                self.log_parser.parse(logs, show_progress=False)
                progress.update(task, advance=batch_size)
