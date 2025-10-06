from collections import defaultdict

from rich.progress import Progress, TaskID

from src.schemas.node import Node

from src.tasks.fetch.raw_block import fetch_raw_block
from src.tasks.parse.block import parse_block
from src.tasks.parse.transaction import parse_transaction
from src.tasks.parse.withdrawal import parse_withdrawal

from src.tasks.fetch.raw_receipt import fetch_raw_receipt
from src.tasks.parse.receipt import parse_receipt
from src.tasks.parse.log import parse_log
from src.tasks.parse.transfer import parse_transfer
from src.tasks.parse.uniswap_v2_event import parse_uniswap_v2_event
from src.tasks.parse.uniswap_v3_event import parse_uniswap_v3_event
from src.tasks.parse.account import parse_account, enrich_account_balance
from src.tasks.parse.pool import parse_pool, enrich_pool_token, enrich_pool_balance
from src.tasks.parse.token import parse_token, enrich_token_info

from src.tasks.fetch.raw_trace import fetch_raw_trace
from src.tasks.parse.trace import parse_trace

from src.tasks.finish import finish

from src.tasks.export.export_sqlite import entity_func as sqlite_entity_func

from src.utils.enumeration import Entity, Exporter

entity_func = {
    Entity.RAW_BLOCK: [fetch_raw_block],
    Entity.BLOCK: [parse_block],
    Entity.TRANSACTION: [parse_transaction],
    Entity.WITHDRAWAL: [parse_withdrawal],
    
    Entity.RAW_RECEIPT: [fetch_raw_receipt],
    Entity.RECEIPT: [parse_receipt],
    Entity.LOG: [parse_log],
    Entity.TRANSFER: [parse_transfer],
    Entity.EVENT: [parse_uniswap_v2_event, parse_uniswap_v3_event],
    Entity.ACCOUNT: [parse_account, enrich_account_balance],
    Entity.POOL: [parse_pool, enrich_pool_token, enrich_pool_balance],
    Entity.TOKEN: [parse_token, enrich_token_info],

    Entity.RAW_TRACE: [fetch_raw_trace],
    Entity.TRACE: [parse_trace]
}
func_func = {
    fetch_raw_block: [],
    parse_block: [fetch_raw_block],
    parse_transaction: [fetch_raw_block],
    parse_withdrawal: [fetch_raw_block],

    fetch_raw_receipt: [],
    parse_receipt: [fetch_raw_receipt],
    parse_log: [fetch_raw_receipt],
    parse_transfer: [parse_log],
    parse_uniswap_v2_event: [parse_log],
    parse_uniswap_v3_event: [parse_log],
    parse_account: [parse_receipt],
    enrich_account_balance: [parse_account],
    parse_pool: [parse_uniswap_v2_event, parse_uniswap_v3_event],
    enrich_pool_token: [parse_pool],
    enrich_pool_balance: [enrich_pool_token],
    parse_token: [enrich_pool_token],
    enrich_token_info: [parse_token],

    fetch_raw_trace: [],
    parse_trace: [fetch_raw_trace]
}

exporter_entity_func = {Exporter.SQLITE: sqlite_entity_func}


def create_node(
    progress: Progress,
    task_id: TaskID,
    rpc_client,
    start_block: int,
    end_block: int,
    entities: list[str],
    exporters: list[str],
):
    nodes = {}
    dag_id = f"{start_block}_{end_block}"
    params = {
        "progress": progress,
        "task_id": task_id,
        "results": defaultdict(list),
        "rpc_client": rpc_client,
        "block_numbers": range(start_block, end_block + 1),
        "batch_size": end_block - start_block + 1,
        "include_transaction": True,  # TODO: fix this
    }

    required_funcs = set()
    for entity in entities:
        required_funcs.update(entity_func[entity])

    required_funcs = list(required_funcs)

    all_node_names = []
    for func in required_funcs:
        dep_funcs = func_func[func]
        dep_names = [f"{dag_id}_{dep.__name__}" for dep in dep_funcs]
        for dep in dep_funcs:
            if dep not in required_funcs:
                required_funcs.append(dep)

        node_name = f"{dag_id}_{func.__name__}"
        all_node_names.append(node_name)
        nodes[node_name] = Node(
            dag_id=dag_id,
            name=node_name,
            func=func,
            kwargs=params,
            task=None,
            dep_nodes=dep_names,
            dep_data=[],
            status="pending",
        )

    for exporter in exporters:
        for entity in entities:
            func = exporter_entity_func[exporter][entity]
            dep_funcs = entity_func[entity]
            dep_names = [f"{dag_id}_{dep.__name__}" for dep in dep_funcs]

            node_name = f"{dag_id}_{exporter}_{func.__name__}"
            all_node_names.append(node_name)
            nodes[node_name] = Node(
                dag_id=dag_id,
                name=node_name,
                func=func,
                kwargs=params,
                task=None,
                dep_nodes=dep_names,
                dep_data=[],
                status="pending",
            )

    nodes[f"{dag_id}_finish"] = Node(
        dag_id=dag_id,
        name=f"{dag_id}_finish",
        func=finish,
        kwargs=params,
        task=None,
        dep_nodes=all_node_names,
        dep_data=[],
        status="pending",
    )

    return nodes
