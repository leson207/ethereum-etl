from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.schemas.python.withdrawal import Withdrawal


class WithdrawalExtractor:
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
                "Withdrawal: ",
                total=(total or len(items)),
                completed=(initial or 0),
            )

            results = []
            for block in items:
                block_number = block["number"]
                for withdrawal in block["withdrawals"]:
                    withdrawal = self.extract(withdrawal, block_number=block_number)
                    results.append(withdrawal.model_dump())

                progress.update(task, advance=batch_size)

            return results

    def extract(self, item: dict, block_number):
        withdrawal = Withdrawal(
            block_number=block_number,
            index=item.get("index"),
            validator_index=item.get("validatorIndex"),
            address=item.get("address"),
            amount=item.get("amount"),
        )

        return withdrawal
