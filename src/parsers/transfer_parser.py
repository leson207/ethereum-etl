from eth_utils import keccak, to_hex
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.exporters.manager import ExportManager
from src.schemas.python.transfer import Transfer
from src.utils.enumeration import EntityType

TRANSFER_EVENT_TEXT_SIGNATURE = "Transfer(address,address,uint256)"  # from, to, value
TRANSFER_EVENT_HEX_SIGNATURE = to_hex(keccak(text=TRANSFER_EVENT_TEXT_SIGNATURE))
TRANSFER_EVENT_HEX_SIGNATURE = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


class TransferParser:
    def __init__(self, exporter: ExportManager):
        self.exporter = exporter

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
                "Transfer: ",
                total=(total or len(items)),
                completed=(initial or 0),
            )

            for block in items:
                for receipt in block:
                    for log in receipt['logs']:
                        if (
                            len(log["topics"]) == 0
                            or log["topics"][0] != TRANSFER_EVENT_HEX_SIGNATURE
                        ):
                            continue

                        transfer = self._parse(log)
                        self.exporter.add_item(EntityType.TRANSFER, transfer.model_dump())
                progress.update(task, advance=batch_size)

    def _parse(self, item: dict):
        from_address = "0x" + item["topics"][1][-40:]
        to_address = "0x" + item["topics"][2][-40:]
        if len(item['topics'])==4:
            value = "0x" + item["topics"][3][-40:]
        else:
            value = item["data"]
        hex_data = item["data"][2:]
        data = [hex_data[i:i+64] for i in range(0, len(hex_data), 64)]

        if len(hex_data)%64!=0:
            print('7'*100)
            print(item)
            print(len(hex_data))
            raise

        if len(data)+len(item["topics"])!=4:
            print('8'*100)
            print(item)
            raise

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
