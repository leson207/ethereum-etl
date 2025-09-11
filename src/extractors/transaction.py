from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.schemas.python.transaction import Transaction


class TransactionExtractor:
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
                "Transaction: ",
                total=(total or len(items)),
                completed=(initial or 0),
            )

            results = []
            for block in items:
                for transaction in block["transactions"]:
                    transaction = self.extract(transaction)
                    results.append(transaction.model_dump())
                
                progress.update(task, advance=batch_size)

            return results

    def extract(self, item: dict):
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
