from tqdm.std import tqdm

from src.schemas.python.token import Token
from src.utils.enumeration import EntityType
from src.utils.progress_bar import get_progress_bar


class TokenParser:
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
            token = self._parse(item)
            self.exporter.add_item(EntityType.TOKEN, [token.model_dump()])

    def _parse(self, item: dict):

        # token = self.token_service.get_token(token_address)
        # token.block_number = block_number
        token = None

        return token
        
        token = Token(
            address=item.get("address"),
            topics=item.get("topics"),
            data=item.get("data"),
            block_hash=item.get("blockHash"),
            block_number=item.get("blockNumber"),
            block_timestamp=item.get("blockTimestamp"),
            transaction_hash=item.get("transactionHash"),
            transaction_index=item.get("transactionIndex"),
            log_index=item.get("logIndex"),
            removed=item.get("removed"),
        )

        return token
