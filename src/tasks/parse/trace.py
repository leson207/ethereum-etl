from src.schemas.python.trace import Trace
from src.utils.enumeration import EntityType


def parse_trace(results: dict):
    for block_trace in results[EntityType.RAW_TRACE]:
        for raw_trace in block_trace["data"]:
            trace = parse(raw_trace)
            results[EntityType.TRACE].append(trace)


def parse(item: dict):
    trace = Trace(
        action=item["action"],
        block_hash=item["blockHash"],
        block_number=item["blockNumber"],
        error=item.get("error"),
        result=item["result"],
        subtraces=item["subtraces"],
        trace_address=item["traceAddress"],
        transaction_hash=item.get("transactionHash"),
        transaction_position=item.get("transactionPosition"),
        type=item["type"],
    )
    return trace.model_dump()
