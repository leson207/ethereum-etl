from tqdm.std import tqdm

from src.schemas.python.transaction import Transaction
from src.utils.enumeration import EntityType
from src.utils.progress_bar import get_progress_bar


class TransactionParser:
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
            transaction = self._parse(item)
            self.exporter.add_item(EntityType.TRANSACTION, [transaction])

    def _parse(self, item: dict):
        transaction = Transaction(
            transaction_type=item.get("type"),
            nonce=item.get("nonce"),
            gas=item.get("gas"),
            max_fee_per_gas=item.get("maxFeePerGas"),
            max_priority_fee_per_gas=item.get("maxPriorityFeePerGas"),
            to_address=item.get("to"),
            value=item.get("value"),
            input=item.get("input"),
            transaction_hash=item.get("hash"),
            block_hash=item.get("blockHash"),
            block_number=item.get("blockNumber"),
            transaction_index=item.get("transactionIndex"),
            from_address=item.get("from"),
            gas_price=item.get("gasPrice"),
            max_fee_per_blob_gas=item.get("maxFeePerBlobGas"),
            blob_versioned_hashes=item.get("blobVersionedHashes"),
        )

        return transaction
