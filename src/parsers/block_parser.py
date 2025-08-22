from tqdm.std import tqdm

from src.schemas.python.block import Block
from src.utils.enumeration import EntityType
from src.utils.progress_bar import get_progress_bar


class BlockParser:
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
            block = self._parse(item)
            self.exporter.add_item(EntityType.BLOCK, [block])

    def _parse(self, item: dict):
        block = Block(
            number=item.get("number"),
            hash=item.get("hash"),
            parent_hash=item.get("parentHash"),
            nonce=item.get("nonce"),
            sha3_uncles=item.get("sha3Uncles"),
            logs_bloom=item.get("logsBloom"),
            transactions_root=item.get("transactionsRoot"),
            state_root=item.get("stateRoot"),
            receipts_root=item.get("receiptsRoot"),
            miner=item.get("miner"),
            difficulty=item.get("difficulty"),
            total_difficulty=item.get("totalDifficulty"),
            size=item.get("size"),
            extra_data=item.get("extraData"),
            gas_limit=item.get("gasLimit"),
            gas_used=item.get("gasUsed"),
            timestamp=item.get("timestamp"),
            base_fee_per_gas=item.get("baseFeePerGas"),
            withdrawals_root=item.get("withdrawalsRoot"),
            blob_gas_used=item.get("blobGasUsed"),
            excess_blob_gas=item.get("excessBlobGas"),
            transaction_count=len(item.get("transactions", [])),
            withdrawal_count=len(item.get("withdrawals", [])),
        )
        return block
