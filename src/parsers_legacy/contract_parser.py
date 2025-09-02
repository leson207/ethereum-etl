from tqdm.std import tqdm

from src.schemas.python.contract import Contract
from src.utils.enumeration import EntityType
from src.utils.progress_bar import get_progress_bar


class ContractParser:
    def __init__(self, exporter):
        self.exporter = exporter

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
            contract = self._parse(item)
            self.exporter.add_item(EntityType.CONTRACT, [contract.model_dump()])

    def _parse(self, item: dict):
        contract = Contract(
            address=item["result"]["address"],
            bytecode=item["result"]["code"],
            block_number=item["block_number"],
            function_sighashes= "",
            is_erc20=True,
            is_erc721=True
        )
        # TODO: use contract service here

        return contract
