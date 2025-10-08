import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor

import uvloop
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.configs.connection_manager import connection_manager
from src.tasks.dag import create_node
from src.tasks.graph import Graph

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


def parse_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-block", type=int)
    parser.add_argument("--end-block", type=int)
    parser.add_argument("--request-batch-size", type=int, default=30)
    parser.add_argument("--pending-queue-size", type=int, default=1000)
    parser.add_argument("--running-queue-size", type=int, default=1000)
    parser.add_argument("--entities", type=str, default=None)
    parser.add_argument("--exporters", type=str, default=None)
    parser.add_argument("--num-workers", type=str, default=4)
    return parser.parse_args()


async def main(
    start_block: int,
    end_block: int,
    request_batch_size: int,
    pending_queue_size: int,
    running_queue_size: int,
    entities: list[str],
    exporters: list[str],
    num_workers: int,
):  
    await connection_manager.init(exporters+ ["rpc", "memgraph"])
    await connection_manager["memgraph"].execute_query("MATCH (n) DETACH DELETE n")

    graph = Graph()

    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        async with asyncio.TaskGroup() as tg:
            with Progress(
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
            ) as progress:
                task_id = progress.add_task(
                    description="Block: ",
                    total=end_block - start_block + 1,
                )
                batch_start_block = start_block

                while progress.tasks[task_id].completed < end_block - start_block + 1:
                    if (
                        graph.pending_count < pending_queue_size
                        and batch_start_block <= end_block
                    ):
                        batch_end_block = min(
                            batch_start_block + request_batch_size - 1, end_block
                        )
                        new_nodes = create_node(
                            progress,
                            task_id,
                            connection_manager["rpc"],
                            connection_manager["memgraph"],
                            batch_start_block,
                            batch_end_block,
                            entities,
                            exporters,
                        )
                        batch_start_block = batch_start_block + request_batch_size

                        graph.add_nodes(new_nodes)

                    if graph.running_count < running_queue_size:
                        await graph.run(tg, pool)

                    await asyncio.sleep(0.1)

    await connection_manager.close()


if __name__ == "__main__":
    args = parse_arg()
    entities = args.entities.split(",") if args.entities else []
    exporters = args.exporters.split(",") if args.exporters else []
    with asyncio.Runner() as runner:
        runner.run(
            main(
                args.start_block,
                args.end_block,
                args.request_batch_size,
                args.pending_queue_size,
                args.running_queue_size,
                entities,
                exporters,
                args.num_workers,
            )
        )

# python -m src.clis.historical --start-block 23170000 --end-block 23170030 \
# --pending-queue-size 1000 --running-queue-size 100 --request-batch-size 30 \
# --entities raw_block,block,transaction,withdrawal --exporters sqlite

# python -m src.clis.historical --start-block 23170000 --end-block 23170030 \
# --pending-queue-size 1000 --running-queue-size 100 --request-batch-size 30 \
# --entities raw_receipt,receipt,log,transfer,event --exporters sqlite

# python -m src.clis.historical --start-block 23170000 --end-block 23170030 \
# --pending-queue-size 1000 --running-queue-size 100 --request-batch-size 30 \
# --entities raw_trace,trace --exporters sqlite