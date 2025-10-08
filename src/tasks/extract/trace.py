from src.utils.enumeration import Entity


def trace_init(results: dict[str, list], **kwargs):
    for raw_trace in results[Entity.RAW_TRACE]:
        trace = _extract(raw_trace["data"])
        results[Entity.TRACE].append(trace)


def _extract(item: dict):
    trace = {
        "action": item["action"],
        "block_hash": item["blockHash"],
        "block_number": item["blockNumber"],
        "error": item.get("error"),
        "result": item["result"],
        "subtraces": item["subtraces"],
        "trace_address": item["traceAddress"],
        "transaction_hash": item.get("transactionHash"),
        "transaction_position": item.get("transactionPosition"),
        "type": item["type"],
    }
    return trace
