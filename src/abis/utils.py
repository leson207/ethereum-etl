from eth_utils import keccak, to_hex


# GPT code
def get_input_type(inp):
    t = inp["type"]

    # Normalize base types
    if t == "uint":
        return "uint256"
    if t == "int":
        return "int256"
    if t == "byte":
        return "bytes1"

    # Tuples (structs)
    if t.startswith("tuple"):
        inner = ",".join(get_input_type(c) for c in inp["components"])
        inner = f"({inner})"
        # Handle tuple[], tuple[][], tuple[3], etc.
        return t.replace("tuple", inner)

    # For everything else (address, bool, string, bytesN, arrays)
    return t


def encode_abi_element(item):
    input_types = [get_input_type(input) for input in item["inputs"]]
    text_signature = f"{item['name']}({','.join(input_types)})"
    hex_signature = to_hex(keccak(text=text_signature))
    return text_signature, hex_signature
