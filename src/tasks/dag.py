from collections import defaultdict

from rich.progress import Progress, TaskID

from src.schemas.node import Node
from src.tasks.export.export_sqlite import entity_func as sqlite_entity_func
from src.tasks.fetch.raw_block import fetch_raw_block
from src.tasks.finish import finish
from src.tasks.parse.block import parse_block
from src.tasks.parse.transaction import parse_transaction
from src.tasks.parse.withdrawal import parse_withdrawal
from src.utils.enumeration import Entity, Exporter

entity_func = {
    Entity.RAW_BLOCK: [fetch_raw_block],
    Entity.BLOCK: [parse_block],
    Entity.TRANSACTION: [parse_transaction],
    Entity.WITHDRAWAL: [parse_withdrawal],
}
func_func = {
    fetch_raw_block: [],
    parse_block: [fetch_raw_block],
    parse_transaction: [fetch_raw_block],
    parse_withdrawal: [fetch_raw_block],
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
        "client": rpc_client,
        "block_numbers": range(start_block, end_block + 1),
        "batch_size": end_block - start_block + 1,
        "include_transaction": True,  # fix this
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
