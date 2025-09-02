from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from src.schemas.python.event import Event
from src.utils.enumeration import EntityType
from src.abis.event import EVENT_HEX_SIGNATURES, decode_event_input


# https://ethereum.stackexchange.com/questions/12553/understanding-logs-and-log-blooms
class EventParser:
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
        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            disable=not show_progress,
        ) as progress:
            task = progress.add_task(
                "Event: ",
                total=(total or len(items)),
                completed=(initial or 0),
            )

            for item in items:
                event = self._parse(item)
                if event:
                    self.exporter.add_item(EntityType.EVENT, [event.model_dump()])
                
                progress.update(task, advance=batch_size)

    def _parse(self, log: dict):
        topics = log["topics"]
        if not topics:
            return None

        dispatch = {
            EVENT_HEX_SIGNATURES["uniswap_v2"]["pair_created"]: self._process_uniswap_v2_pair_created,
            EVENT_HEX_SIGNATURES["uniswap_v3"]["pool_created"]: self._process_uniswap_v3_pool_created,
            EVENT_HEX_SIGNATURES["uniswap_v2"]["mint"]: self._process_uniswap_v2_mint,
            EVENT_HEX_SIGNATURES["uniswap_v3"]["mint"]: self._process_uniswap_v3_mint,
            EVENT_HEX_SIGNATURES["uniswap_v2"]["burn"]: self._process_uniswap_v2_burn,
            EVENT_HEX_SIGNATURES["uniswap_v3"]["burn"]: self._process_uniswap_v3_burn,
            EVENT_HEX_SIGNATURES["uniswap_v2"]["swap"]: self._process_uniswap_v2_swap,
            EVENT_HEX_SIGNATURES["uniswap_v3"]["swap"]: self._process_uniswap_v3_swap,
        }

        handler = dispatch.get(topics[0].casefold())
        if not handler:
            return None

        data = handler(log)
        if not data:
            print('a')
            return None
        
        event = Event(
            type=data["type"],
            dex=data["dex"],
            pool_address=data["pool_address"],
            amount0_in=data["amount0_in"],
            amount1_in=data["amount1_in"],
            amount0_out=data["amount0_out"],
            amount1_out=data["amount1_out"],
            transaction_hash=log["transaction_hash"],
            log_index=log["log_index"],
            block_number=log["block_number"],
        )
        return event

    def _process_uniswap_v2_pair_created(self, log):
        decode = decode_event_input("uniswap_v2", "pair_created", log['topics'], log['data'])
        if not decode:
            return None
        
        return {
            "type": "pair_created",
            "dex": "uniswap_v2",
            "pool_address": decode["pair"],
            "amount0_in": 0,
            "amount1_in": 0,
            "amount0_out": 0,
            "amount1_out": 0,
        }

    def _process_uniswap_v3_pool_created(self, log):
        decode = decode_event_input("uniswap_v3", "pool_created", log['topics'], log['data'])
        if not decode:
            return None

        return {
            "type": "pair_created",
            "dex": "uniswap_v3",
            "pool_address": decode["pool"],
            "amount0_in": 0,
            "amount1_in": 0,
            "amount0_out": 0,
            "amount1_out": 0,
        }

    def _process_uniswap_v2_mint(self, log):
        decode = decode_event_input("uniswap_v2", "mint", log['topics'], log['data'])
        if not decode:
            return None
        
        return {
            "type": "mint",
            "dex": "uniswap_v2",
            "pool_address": log["address"],
            "amount0_in": decode["amount0"],
            "amount1_in": decode["amount1"],
            "amount0_out": 0,
            "amount1_out": 0,
        }

    def _process_uniswap_v3_mint(self, log):
        decode = decode_event_input("uniswap_v3", "mint", log['topics'], log['data'])
        if not decode:
            return None
        
        return {
            "type": "mint",
            "dex": "uniswap_v3",
            "pool_address": log["address"],
            "amount0_in": decode["amount0"],
            "amount1_in": decode["amount1"],
            "amount0_out": 0,
            "amount1_out": 0,
        }

    def _process_uniswap_v2_burn(self, log):
        decode = decode_event_input("uniswap_v2", "burn", log['topics'], log['data'])
        if not decode:
            return None

        return {
            "type": "burn",
            "dex": "uniswap_v2",
            "pool_address": log["address"],
            "amount0_in": 0,
            "amount1_in": 0,
            "amount0_out": decode["amount0"],
            "amount1_out": decode["amount1"],
        }

    def _process_uniswap_v3_burn(self, log):
        decode = decode_event_input("uniswap_v3", "burn", log['topics'], log['data'])
        if not decode:
            return None

        return {
            "type": "burn",
            "dex": "uniswap_v3",
            "pool_address": log["address"],
            "amount0_in": 0,
            "amount1_in": 0,
            "amount0_out": decode["amount0"],
            "amount1_out": decode["amount1"],
        }

    def _process_uniswap_v2_swap(self, log):
        decode = decode_event_input("uniswap_v2", "swap", log['topics'], log['data'])
        if not decode:
            return None

        # amount0In can amount0Out can both !=0
        # amount1In can amount1Out can both !=0
        return {
            "type": "swap",
            "dex": "uniswap_v2",
            "pool_address": log["address"],
            "amount0_in": decode["amount0In"],
            "amount1_in": decode["amount1In"],
            "amount0_out": decode["amount0Out"],
            "amount1_out": decode["amount1Out"],
        }

    def _process_uniswap_v3_swap(self, log):
        decode = decode_event_input("uniswap_v3", "swap", log['topics'], log['data'])
        if not decode:
            return None

        amount_in = decode["amount0"]
        amount_out = decode["amount1"]

        if amount_in < 0:
            # buy
            amount0_in = 0
            amount1_in = amount_out
            amount0_out = -amount_in
            amount1_out = 0
        else:
            # sell
            amount0_in = amount_in
            amount1_in = 0
            amount0_out = 0
            amount1_out = -amount_out

        return {
            "type": "swap",
            "dex": "uniswap_v3",
            "pool_address": log["address"],
            "amount0_in": amount0_in,
            "amount1_in": amount1_in,
            "amount0_out": amount0_out,
            "amount1_out": amount1_out,
        }
