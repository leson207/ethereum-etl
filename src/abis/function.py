from eth_abi import decode
from eth_utils import keccak, to_hex

FUNCTION_ABI = {
    # https://gist.github.com/veox/8800debbf56e24718f9f483e1e40c35c
    "erc20": {
        "token0": {
            "constant": True,
            "inputs": [],
            "name": "token0",
            "outputs": [
                {"internalType": "address", "name": "token_address", "type": "address"}
            ],
            "payable": False,
            "stateMutability": "view",
            "type": "function",
        },
        "token1": {
            "constant": True,
            "inputs": [],
            "name": "token1",
            "outputs": [
                {"internalType": "address", "name": "token_address", "type": "address"}
            ],
            "payable": False,
            "stateMutability": "view",
            "type": "function",
        },
        "balanceOf": {
            "constant": True,
            "inputs": [{"name": "_owner", "type": "address"}],
            "name": "balanceOf",
            "outputs": [{"name": "balance", "type": "uint256"}],
            "payable": False,
            "stateMutability": "view",
            "type": "function",
        },
        "name": {
            "constant": True,
            "inputs": [],
            "name": "name",
            "outputs": [{"name": "name", "type": "string"}],
            "payable": False,
            "stateMutability": "view",
            "type": "function",
        },
        "symbol": {
            "constant": True,
            "inputs": [],
            "name": "symbol",
            "outputs": [{"name": "symbol", "type": "string"}],
            "payable": False,
            "stateMutability": "view",
            "type": "function",
        },
        "decimals": {
            "constant": True,
            "inputs": [],
            "name": "decimals",
            "outputs": [{"name": "decimals", "type": "uint8"}],
            "payable": False,
            "stateMutability": "view",
            "type": "function",
        },
        "totalSupply": {
            "constant": True,
            "inputs": [],
            "name": "totalSupply",
            "outputs": [{"name": "totalSupply", "type": "uint256"}],
            "payable": False,
            "stateMutability": "view",
            "type": "function",
        },
    }
}


FUNCTION_TEXT_SIGNATURES = {}
FUNCTION_HEX_SIGNATURES = {}

for erc in FUNCTION_ABI:
    FUNCTION_TEXT_SIGNATURES[erc] = {}
    FUNCTION_HEX_SIGNATURES[erc] = {}

    for func, schema in FUNCTION_ABI[erc].items():
        input_types = [input["type"] for input in schema["inputs"]]

        text_signature = f"{schema['name']}({','.join(input_types)})"
        hex_signature = to_hex(keccak(text=text_signature))

        FUNCTION_TEXT_SIGNATURES[erc][func] = text_signature
        FUNCTION_HEX_SIGNATURES[erc][func] = hex_signature


def decode_function_output(erc, function_name, outputs):
    decoded = {}

    output_schemas = FUNCTION_ABI[erc][function_name]["outputs"]
    output_names = [i["name"] for i in output_schemas]
    output_types = [i["type"] for i in output_schemas]

    values = decode(output_types, bytes.fromhex(outputs[2:]))
    for name, val in zip(output_names, values):
        decoded[name] = val

    return decoded
