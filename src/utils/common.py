import orjson


def dump_json(path, data):
    with open(path, "wb") as f:
        f.write(orjson.dumps(data, option=orjson.OPT_INDENT_2))


def serialize_dict_value(d: dict):
    text = ""
    for value in d.values():
        text = text + f"_{value}"

    return text


def hex_to_dec(hex, signed=False):
    if not signed:
        return int(hex, 16)

    return 

def empty_to_none(val):
    if val in ("", [], {}, ()):
        return None

    return val
