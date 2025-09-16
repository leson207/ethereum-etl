from eth_utils import keccak, to_hex
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.schemas.python.transfer import Transfer

TRANSFER_EVENT_TEXT_SIGNATURE = "Transfer(address,address,uint256)"  # from, to, value
TRANSFER_EVENT_HEX_SIGNATURE = to_hex(keccak(text=TRANSFER_EVENT_TEXT_SIGNATURE))
TRANSFER_EVENT_HEX_SIGNATURE = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)


class TransferExtractor:
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
                "Transfer: ",
                total=(total or len(items)),
                completed=(initial or 0),
            )

            results = []
            for block in items:
                for receipt in block:
                    for log in receipt["logs"]:
                        if (
                            len(log["topics"]) == 0
                            or log["topics"][0] != TRANSFER_EVENT_HEX_SIGNATURE
                        ):
                            continue

                        transfer = self.extract(log)
                        results.append(transfer.model_dump())

                progress.update(task, advance=batch_size)

            return results

    def extract(self, item: dict):
        params = item["topics"][1:]
        for i in range((len(item['data'])-2)//64):
            params.append(item['data'][2+i:2+i+64])

        if len(params)!=3:
            raise

        from_address = "0x" + params[0][-40:]
        to_address = "0x" + params[1][-40:]
        value = params[2]

        transfer = Transfer(
            contract_address=item["address"],
            from_address=from_address,
            to_address=to_address,
            value=value,
            transaction_hash=item["transactionHash"],
            log_index=item["logIndex"],
            block_number=item["blockNumber"],
        )

        return transfer