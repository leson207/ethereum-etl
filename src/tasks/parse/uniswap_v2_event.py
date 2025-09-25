from src.abis.event import EVENT_HEX_SIGNATURES, decode_event_input
from src.schemas.python.event import Event
from src.utils.enumeration import EntityType


def parse_uniswap_v2_event(results):
    for log in results[EntityType.LOG]:
        event = parse(log)
        results[EntityType.EVENT].append(event)


# https://ethereum.stackexchange.com/questions/12553/understanding-logs-and-log-blooms
def parse(log: dict):
    topics = log["topics"]
    if not topics:
        return None

    data = None

    match topics[0].casefold():
        case sig if (
            sig == EVENT_HEX_SIGNATURES["uniswap_v2"]["pair_created"].casefold()
        ):
            data = _extract_pair_created(log)
        case sig if sig == EVENT_HEX_SIGNATURES["uniswap_v2"]["mint"].casefold():
            data = _extract_mint(log)
        case sig if sig == EVENT_HEX_SIGNATURES["uniswap_v2"]["burn"].casefold():
            data = _extract_burn(log)
        case sig if sig == EVENT_HEX_SIGNATURES["uniswap_v2"]["swap"].casefold():
            data = _extract_swap(log)
        case _:
            return None

    if not data:
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


def _extract_pair_created(log):
    decode = decode_event_input(
        "uniswap_v2", "pair_created", log["topics"], log["data"]
    )
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


def _extract_mint(log):
    decode = decode_event_input("uniswap_v2", "mint", log["topics"], log["data"])
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


def _extract_burn(log):
    decode = decode_event_input("uniswap_v2", "burn", log["topics"], log["data"])
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


def _extract_swap(log):
    decode = decode_event_input("uniswap_v2", "swap", log["topics"], log["data"])
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
