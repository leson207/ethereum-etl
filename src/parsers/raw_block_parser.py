from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.schemas.python.block import Block
from src.schemas.python.transaction import Transaction
from src.schemas.python.withdrawal import Withdrawal
from src.utils.enumeration import EntityType


class RawBlockParser:
    def __init__(
        self,
        exporter,
        target=[EntityType.BLOCK, EntityType.TRANSACTION, EntityType.WITHDRAWAL],
    ):
        self.exporter = exporter
        self.target = target

    def parse(
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
                "Block - Transaction - Withdrawal: ",
                total=(total or len(items)),
                completed=(initial or 0),
            )

            for raw_block in items:
                block = self._parse_block(raw_block)

                if EntityType.BLOCK in self.target:
                    self.exporter.add_item(EntityType.BLOCK, block.model_dump())

                if EntityType.TRANSACTION in self.target:
                    for raw_transaction in raw_block.get("transactions", []):
                        transaction = self._parse_transaction(raw_transaction)
                        self.exporter.add_item(
                            EntityType.TRANSACTION, transaction.model_dump()
                        )

                if EntityType.WITHDRAWAL in self.target:
                    for raw_withdrawal in raw_block.get("withdrawals", []):
                        withdrawal = self._parse_withdrawal(
                            raw_withdrawal,
                            block_hash=block.hash,
                            block_number=block.number,
                        )
                        self.exporter.add_item(
                            EntityType.WITHDRAWAL, withdrawal.model_dump()
                        )

                progress.update(task, advance=batch_size)

    def _parse_block(self, item: dict):
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

    def _parse_transaction(self, item: dict):
        transaction = Transaction(
            type=item.get("type"),
            chain_id=item.get("chainId"),
            nonce=item.get("nonce"),
            gas=item.get("gas"),
            max_fee_per_gas=item.get("maxFeePerGas"),
            max_priority_fee_per_gas=item.get("maxPriorityFeePerGas"),
            to_address=item.get("to"),
            value=item.get("value"),
            access_list=item.get("accessList"),
            authorization_list=item.get("authorizationList"),
            input=item.get("input"),
            r=item.get("r"),
            s=item.get("s"),
            y_parity=item.get("yParity"),
            v=item.get("v"),
            hash=item.get("hash"),
            block_hash=item.get("blockHash"),
            block_number=item.get("blockNumber"),
            transaction_index=item.get("transactionIndex"),
            from_address=item.get("from"),
            gas_price=item.get("gasPrice"),
            max_fee_per_blob_gas=item.get("maxFeePerBlobGas"),
            blob_versioned_hashes=item.get("blobVersionedHashes"),
        )
        return transaction

    def _parse_withdrawal(self, item: dict, **kwargs):
        withdrawal = Withdrawal(
            block_hash=kwargs.get("block_hash"),
            block_number=kwargs.get("block_number"),
            index=item.get("index"),
            validator_index=item.get("validatorIndex"),
            address=item.get("address"),
            amount=item.get("amount"),
        )

        return withdrawal
