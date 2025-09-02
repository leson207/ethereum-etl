from tqdm.std import tqdm

from src.utils.progress_bar import get_progress_bar


class BaseParser:
    def __init__(self, exporter: list):
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
            receipt = self._parse(item)
            self.exporter.append(receipt.model_dump())

    def _parse(self, item: dict):
        pass
