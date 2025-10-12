from collections import defaultdict

from rich.progress import Progress, TaskID

from src.schemas.node import Node

from src.tasks.fetch.raw_block import raw_block_init
from src.tasks.extract.block import block_init
from src.tasks.extract.transaction import transaction_init
from src.tasks.extract.withdrawal import withdrawal_init

from src.tasks.fetch.raw_receipt import raw_receipt_init
from src.tasks.extract.receipt import receipt_init
from src.tasks.extract.log import log_init
from src.tasks.extract.transfer import transfer_init
from src.tasks.extract.uniswap_v2_event import uniswap_v2_event_init
from src.tasks.extract.uniswap_v3_event import uniswap_v3_event_init
from src.tasks.extract.account import account_init_address, account_enrich_balance
from src.tasks.extract.pool import pool_init_address, pool_enrich_token_address, pool_enrich_token_balance, pool_update_graph, pool_enrich_token_price
from src.tasks.extract.token import token_init_address, token_enrich_info, token_update_graph

from src.tasks.fetch.raw_trace import raw_trace_init
from src.tasks.extract.trace import trace_init

from src.tasks.finish import finish

from src.tasks.export.sqlite import entity_func as sqlite_entity_func

from src.utils.enumeration import Entity, Exporter

entity_func = {
    Entity.RAW_BLOCK: [raw_block_init],
    Entity.BLOCK: [block_init],
    Entity.TRANSACTION: [transaction_init],
    Entity.WITHDRAWAL: [withdrawal_init],
    
    Entity.RAW_RECEIPT: [raw_receipt_init],
    Entity.RECEIPT: [receipt_init],
    Entity.LOG: [log_init],
    Entity.TRANSFER: [transfer_init],
    Entity.EVENT: [uniswap_v2_event_init, uniswap_v3_event_init],
    Entity.ACCOUNT: [account_init_address, account_enrich_balance],
    Entity.POOL: [pool_init_address, pool_enrich_token_address, pool_enrich_token_balance, pool_enrich_token_balance, pool_update_graph, pool_enrich_token_price],
    Entity.TOKEN: [token_init_address, token_enrich_info, token_update_graph],

    Entity.RAW_TRACE: [raw_trace_init],
    Entity.TRACE: [trace_init]
}
func_func = {
    raw_block_init: [],
    block_init: [raw_block_init],
    transaction_init: [raw_block_init],
    withdrawal_init: [raw_block_init],

    raw_receipt_init: [],
    receipt_init: [raw_receipt_init],
    log_init: [raw_receipt_init],
    transfer_init: [log_init],
    uniswap_v2_event_init: [log_init],
    uniswap_v3_event_init: [log_init],
    account_init_address: [receipt_init],
    account_enrich_balance: [account_init_address],
    pool_init_address: [uniswap_v2_event_init, uniswap_v3_event_init],
    pool_enrich_token_address: [pool_init_address],
    pool_enrich_token_balance: [pool_enrich_token_address],
    pool_update_graph: [pool_enrich_token_balance],
    pool_enrich_token_price: [token_update_graph, pool_update_graph],
    
    token_init_address: [pool_enrich_token_address],
    token_enrich_info: [token_init_address],
    token_update_graph: [token_enrich_info],

    raw_trace_init: [],
    trace_init: [raw_trace_init]
}

exporter_entity_func = {Exporter.SQLITE: sqlite_entity_func}


def create_node(
    progress: Progress,
    task_id: TaskID,
    rpc_client,
    graph_client,
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
        "graph_client": graph_client,
        "block_numbers": range(start_block, end_block + 1),
        "batch_size": end_block - start_block + 1,
        "include_transaction": Entity.TRANSACTION in entities or Entity.WITHDRAWAL in entities
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
