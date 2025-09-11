import orjson


def dump_json(path, data):
    with open(path, "wb") as f:
        f.write(orjson.dumps(data, option=orjson.OPT_INDENT_2))


def serialize_dict_value(d: dict):
    text = ""
    for value in d.values():
        text = text + f"_{value}"

    return text
