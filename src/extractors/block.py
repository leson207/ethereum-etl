from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.exporters.manager import ExportManager
from src.schemas.python.block import Block
from src.utils.enumeration import EntityType


class BlockExtractor:
    def __init__(self, exporter: ExportManager):
        self.exporter = exporter

    def run(
        self,
        items: list[dict],
        initial=None,
        total=None,
        batch_size=1,
        show_progress=True,
    ):
        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            disable=not show_progress,
        ) as progress:
            task = progress.add_task(
                "Block: ",
                total=(total or len(items)),
                completed=(initial or 0),
            )

            for item in items:
                block = self.extract(item)
                self.exporter.add_item(EntityType.BLOCK, block.model_dump())
                progress.update(task, advance=batch_size)

    def extract(self, item: dict):
        block = Block(
            hash=item.get("hash"),
            parent_hash=item.get("parentHash"),
            sha3_uncles=item.get("sha3Uncles"),
            miner=item.get("miner"),
            state_root=item.get("stateRoot"),
            transactions_root=item.get("transactionsRoot"),
            receipts_root=item.get("receiptsRoot"),
            logs_bloom=item.get("logsBloom"),
            difficulty=item.get("difficulty"),
            number=item.get("number"),
            gas_limit=item.get("gasLimit"),
            gas_used=item.get("gasUsed"),
            timestamp=item.get("timestamp"),
            extra_data=item.get("extraData"),
            mix_hash=item.get("mixHash"),
            nonce=item.get("nonce"),
            base_fee_per_gas=item.get("baseFeePerGas"),
            withdrawals_root=item.get("withdrawalsRoot"),
            blob_gas_used=item.get("blobGasUsed"),
            excess_blob_gas=item.get("excessBlobGas"),
            parent_beacon_block_root=item.get("parentBeaconBlockRoot"),
            requests_hash=item.get("requestsHash"),
            size=item.get("size"),
            uncles=item.get("uncles"),
            total_difficulty=item.get("totalDifficulty"),
            transaction_count=len(item.get("transactions", [])),
            withdrawal_count=len(item.get("withdrawals", [])),
        )
        return block
