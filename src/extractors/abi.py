import orjson
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.abis.utils import encode_abi_element
from src.schemas.python.abi import ABI


class AbiExtractor:
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
                "ABI: ",
                total=(total or len(items)),
                completed=(initial or 0),
            )

            results = []
            for abi_string in items:
                abis = self.extract(abi_string)
                results.extend([abi.model_dump() for abi in abis])

                progress.update(task, advance=batch_size)
            
            return results

    def extract(self, abi_string: str):
        if abi_string == "Contract source code not verified":
            # TODO: this may mean account, check and fix contract parser if needed
            return []
        try:
            abi_dict = orjson.loads(abi_string)
        except:
            print(abi_string)
            raise

        abis = []
        for item in abi_dict:
            if item["type"] not in ["event"]:
                continue

            text_signature, hex_signature = encode_abi_element(item)
            abi = ABI(
                text_signature=text_signature, hex_signature=hex_signature, data=item
            )
            abis.append(abi)

        return abis
