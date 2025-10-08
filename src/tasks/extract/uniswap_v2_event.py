from src.abis.event import EVENT_HEX_SIGNATURES, decode_event_input
from src.utils.enumeration import Entity


def uniswap_v2_event_init(results: dict[str, list], **kwargs):
    for log in results[Entity.LOG]:
        event = _extract(log)
        if event:
            results[Entity.EVENT].append(event)


# https://ethereum.stackexchange.com/questions/12553/understanding-logs-and-log-blooms
def _extract(log: dict):
    topics = log["topics"]
    if not topics:
        return None

    event = None

    match topics[0].casefold():
        case sig if (
            sig == EVENT_HEX_SIGNATURES["uniswap_v2"]["pair_created"].casefold()
        ):
            event = _extract_pair_created(log)
        case sig if sig == EVENT_HEX_SIGNATURES["uniswap_v2"]["mint"].casefold():
            event = _extract_mint(log)
        case sig if sig == EVENT_HEX_SIGNATURES["uniswap_v2"]["burn"].casefold():
            event = _extract_burn(log)
        case sig if sig == EVENT_HEX_SIGNATURES["uniswap_v2"]["swap"].casefold():
            event = _extract_swap(log)
        case _:
            return None

    if not event:
        return None

    event["transaction_hash"] = log["transaction_hash"]
    event["log_index"] = log["log_index"]
    event["block_number"] = log["block_number"]

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

    # amount0In and amount0Out can both !=0
    # amount1In and amount1Out can both !=0
    return {
        "type": "swap",
        "dex": "uniswap_v2",
        "pool_address": log["address"],
        "amount0_in": decode["amount0In"],
        "amount1_in": decode["amount1In"],
        "amount0_out": decode["amount0Out"],
        "amount1_out": decode["amount1Out"],
    }
