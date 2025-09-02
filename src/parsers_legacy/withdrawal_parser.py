from rich.progress import Progress

from src.schemas.python.withdrawal import Withdrawal
from src.utils.enumeration import EntityType


class WithdrawalParser:
    def __init__(self, exporter):
        self.exporter = exporter

    def parse(
        self,
        items: list[dict],
        initial=None,
        total=None,
        batch_size=1,
        show_progress=True,
        **kwargs,
    ):
        with Progress(disable=not show_progress) as progress:
            task = progress.add_task(
                "Withdrawal: ",
                total=(total or len(items)),
                completed=(initial or 0),
            )

            for item in items:
                withdrawal = self._parse(item, **kwargs)
                self.exporter.add_item(EntityType.WITHDRAWAL, [withdrawal.model_dump()])
                progress.update(task, advance=batch_size)

    def _parse(self, item: dict, **kwargs):
        withdrawal = Withdrawal(
            block_hash=kwargs.get("block_hash"),
            block_number=kwargs.get("block_number"),
            index=item.get("index"),
            validator_index=item.get("validatorIndex"),
            address=item.get("address"),
            amount=item.get("amount"),
        )

        return withdrawal
