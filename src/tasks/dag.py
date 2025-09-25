from collections import defaultdict

from rich.progress import Progress, TaskID

from src.tasks.export.export_sqlite import entity_task as sqlite_entity_task
from src.tasks.fetch.raw_block import fetch_raw_block
from src.tasks.finish import finish
from src.tasks.parse.block import parse_block
from src.tasks.parse.transaction import parse_transaction
from src.tasks.parse.withdrawal import parse_withdrawal
from src.utils.enumeration import Entity, Exporter

entity_task = {
    Entity.RAW_BLOCK: [fetch_raw_block],
    Entity.BLOCK: [parse_block],
    Entity.TRANSACTION: [parse_transaction],
    Entity.WITHDRAWAL: [parse_withdrawal],
}
task_task = {
    fetch_raw_block: [],
    parse_block: [fetch_raw_block],
    parse_transaction: [fetch_raw_block],
    parse_withdrawal: [fetch_raw_block],
}

exporter_entity_task = {Exporter.SQLITE: sqlite_entity_task}


def create_task(
    progress: Progress,
    task_id: TaskID,
    rpc_client,
    start_block: int,
    end_block: int,
    entities: list[str],
    exporters: list[str],
):
    tasks = {}
    dag_id = f"{start_block}_{end_block}"
    params = {
        "progress": progress,
        "task_id": task_id,
        "results": defaultdict(list),
        "client": rpc_client,
        "block_numbers": range(start_block, end_block + 1),
        "batch_size": end_block - start_block + 1,
        "include_transaction": True,
    }

    required_tasks = set()
    for entity in entities:
        required_tasks.update(entity_task[entity])

    required_tasks = list(required_tasks)

    all_task_names = []
    for task in required_tasks:
        dep_tasks = task_task[task]
        dep_names = [f"{dag_id}_{dep.__name__}" for dep in dep_tasks]
        for dep in dep_tasks:
            if dep not in required_tasks:
                required_tasks.append(dep)

        name = f"{dag_id}_{task.__name__}"
        all_task_names.append(name)
        tasks[name] = (task, params, dep_names)

    for exporter in exporters:
        for entity in entities:
            task = exporter_entity_task[exporter][entity]
            dep_tasks = entity_task[entity]
            dep_names = [f"{dag_id}_{dep.__name__}" for dep in dep_tasks]

            name = f"{dag_id}_{exporter}_{task.__name__}"
            all_task_names.append(name)
            tasks[name] = (task, params, dep_names)

    tasks[f"{dag_id}_finish"] = (finish, params, all_task_names)

    return tasks
