from tqdm.std import tqdm

from src.schemas.python.withdrawal import Withdrawal
from src.utils.enumeration import EntityType
from src.utils.progress_bar import get_progress_bar


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
        p_bar = get_progress_bar(
            tqdm,
            items,
            initial=(initial or 0) // batch_size,
            total=(total or len(items)) // batch_size,
            show=show_progress,
        )
        for item in p_bar:
            withdrawal = self._parse(item, **kwargs)
            self.exporter.add_item(EntityType.WITHDRAWAL, [withdrawal])

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
