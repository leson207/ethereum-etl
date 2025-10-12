from eth_abi import decode
from eth_utils import keccak, to_hex

EVENT_ABI = {
    # https://github.com/Uniswap/v2-core/blob/master/contracts/interfaces/IUniswapV2Pair.sol
    # https://github.com/Uniswap/v2-core/blob/master/contracts/interfaces/IUniswapV2Factory.sol
    "uniswap_v2": {
        "swap": {
            "anonymous": False,
            "inputs": [
                {"type": "address", "name": "sender", "indexed": True},
                {"type": "uint256", "name": "amount0In", "indexed": False},
                {"type": "uint256", "name": "amount1In", "indexed": False},
                {"type": "uint256", "name": "amount0Out", "indexed": False},
                {"type": "uint256", "name": "amount1Out", "indexed": False},
                {"type": "address", "name": "to", "indexed": True},
            ],
            "name": "Swap",
            "type": "event",
        },
        "burn": {
            "anonymous": False,
            "inputs": [
                {"type": "address", "name": "sender", "indexed": True},
                {"type": "uint256", "name": "amount0", "indexed": False},
                {"type": "uint256", "name": "amount1", "indexed": False},
                {"type": "address", "name": "to", "indexed": True},
            ],
            "name": "Burn",
            "type": "event",
        },
        "mint": {
            "anonymous": False,
            "inputs": [
                {"type": "address", "name": "sender", "indexed": True},
                {"type": "uint256", "name": "amount0", "indexed": False},
                {"type": "uint256", "name": "amount1", "indexed": False},
            ],
            "name": "Mint",
            "type": "event",
        },
        "pair_created": {
            "anonymous": False,
            "inputs": [
                {"type": "address", "name": "token0", "indexed": True},
                {"type": "address", "name": "token1", "indexed": True},
                {"type": "address", "name": "pair", "indexed": False},
                {
                    "type": "uint256",
                    "name": "pairIndex",
                    "indexed": False,
                },  # couter of pair created by the factory contract include this. | uint short for uint256
            ],
            "name": "PairCreated",
            "type": "event",
        },
    },
    # https://github.com/Uniswap/v3-core/blob/main/contracts/interfaces/pool/IUniswapV3PoolEvents.sol
    # https://github.com/Uniswap/v3-core/blob/main/contracts/interfaces/IUniswapV3Factory.sol
    "uniswap_v3": {
        "swap": {
            "anonymous": False,
            "inputs": [
                {"type": "address", "name": "sender", "indexed": True},
                {"type": "address", "name": "recipient", "indexed": True},
                {"type": "int256", "name": "amount0", "indexed": False},
                {"type": "int256", "name": "amount1", "indexed": False},
                {"type": "uint160", "name": "sqrtPriceX96", "indexed": False},
                {"type": "uint128", "name": "liquidity", "indexed": False},
                {"type": "int24", "name": "tick", "indexed": False},
            ],
            "name": "Swap",
            "type": "event",
        },
        "burn": {
            "anonymous": False,
            "inputs": [
                {"type": "address", "name": "owner", "indexed": True},
                {"type": "int24", "name": "tickLower", "indexed": True},
                {"type": "int24", "name": "tickUpper", "indexed": True},
                {"type": "uint128", "name": "amount", "indexed": False},
                {"type": "uint256", "name": "amount0", "indexed": False},
                {"type": "uint256", "name": "amount1", "indexed": False},
            ],
            "name": "Burn",
            "type": "event",
        },
        "mint": {
            "anonymous": False,
            "inputs": [
                {"type": "address", "name": "sender", "indexed": False},
                {"type": "address", "name": "owner", "indexed": True},
                {"type": "int24", "name": "tickLower", "indexed": True},
                {"type": "int24", "name": "tickUpper", "indexed": True},
                {"type": "uint128", "name": "amount", "indexed": False},
                {"type": "uint256", "name": "amount0", "indexed": False},
                {"type": "uint256", "name": "amount1", "indexed": False},
            ],
            "name": "Mint",
            "type": "event",
        },
        "pool_created": {
            "anonymous": False,
            "inputs": [
                {"type": "address", "name": "token0", "indexed": True},
                {"type": "address", "name": "token1", "indexed": True},
                {"type": "uint24", "name": "fee", "indexed": True},
                {"type": "int24", "name": "tickSpacing", "indexed": False},
                {"type": "address", "name": "pool", "indexed": False},
            ],
            "name": "PoolCreated",
            "type": "event",
        },
    },
}

EVENT_TEXT_SIGNATURES = {}
EVENT_HEX_SIGNATURES = {}

for dex in EVENT_ABI:
    EVENT_HEX_SIGNATURES[dex] = {}
    EVENT_TEXT_SIGNATURES[dex] = {}
    for event, schema in EVENT_ABI[dex].items():
        input_types = [input["type"] for input in schema["inputs"]]

        text_signature = f"{schema['name']}({','.join(input_types)})"
        hex_signature = to_hex(keccak(text=text_signature))

        EVENT_TEXT_SIGNATURES[dex][event] = text_signature
        EVENT_HEX_SIGNATURES[dex][event] = hex_signature

# This use for uniswapv2 like transfer
def decode_event_input_specific(dex, event, topics, data):
    inputs = EVENT_ABI[dex][event]["inputs"]

    indexed_inputs = [input for input in inputs if input["indexed"]]
    other_inputs = [input for input in inputs if not input["indexed"]]

    # different event may share the same keccak hash and indexed field do not affect the hash
    # compare indexed len and topics length to ensure they are the same event
    if len(indexed_inputs) + 1 != len(topics):
        return None

    decoded = {}
    # start after topic[0] (event signature)
    for input_def, topic in zip(indexed_inputs, topics[1:]):
        # Indexed fields are in topics
        # Addresses are last 40 hex chars
        if input_def["type"] == "address":
            decoded[input_def["name"]] = "0x" + topic[-40:]
        else:
            # decode numeric indexed (rare) from hex
            decoded[input_def["name"]] = int(topic, 16)

    # Process non-indexed fields from data
    if other_inputs:
        input_types = [i["type"] for i in other_inputs]
        values = decode(input_types, bytes.fromhex(data[2:]))
        for i, val in enumerate(values):
            decoded[other_inputs[i]["name"]] = val

    return decoded


def decode_event_input_universal(dex, event, topics, data):
    inputs = EVENT_ABI[dex][event]["inputs"]

    decoded = {}
    topic_index = 1  # start after topic[0] (event signature)

    # indexed input is in 'topics', but some wrield case it in 'data' instead, i assume it still follow the listing order of abi
    for input_def in inputs:
        if topic_index == len(topics):
            break
        if input_def["indexed"]:
            # Indexed fields are in topics
            # Addresses are last 40 hex chars
            if input_def["type"] == "address":
                decoded[input_def["name"]] = "0x" + topics[topic_index][-40:]
            else:
                # decode numeric indexed (rare) from hex
                decoded[input_def["name"]] = int(topics[topic_index], 16)

            topic_index += 1

    # Process non-indexed fields from data
    other_inputs = [i for i in inputs if i["name"] not in decoded]
    if other_inputs:
        input_types = [i["type"] for i in other_inputs]
        values = decode(input_types, bytes.fromhex(data[2:]))
        for i, val in enumerate(values):
            decoded[other_inputs[i]["name"]] = val

    return decoded
