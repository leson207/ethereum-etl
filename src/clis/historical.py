import argparse
import asyncio
import contextvars
import functools
import inspect
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

from src.clients.rpc_client import RpcClient
from src.logger import logger
from src.tasks.dag import create_task

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


def parse_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-block", type=int)
    parser.add_argument("--end-block", type=int)
    parser.add_argument("--process-batch-size", type=int, default=1000)
    parser.add_argument("--request-batch-size", type=int, default=30)
    parser.add_argument("--entities", type=str, default=None)
    parser.add_argument("--exporters", type=str, default=None)
    parser.add_argument("--num-workers", type=str, default=4)
    return parser.parse_args()


async def to_thread(func, /, *args, executor=None, **kwargs):
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)

    return await loop.run_in_executor(executor, func_call)


async def main(
    start_block: int,
    end_block: int,
    process_batch_size: int,
    request_batch_size: int,
    entities: list[str],
    exporters: list[str],
    num_workers: int,
):
    # Init connection manager
    # Prepare exporter?
    rpc_client = RpcClient()
    res = await rpc_client.get_web3_client_version()
    logger.info(f"Web3 Client Version: {res[0]['result']}")

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

                # should i group 3 of them into something
                pending = {}  # {name: (func, args, dependencies)}
                running = {}  # {name: task}
                done = {}  # {name: bool}
                while progress.tasks[task_id].completed < end_block - start_block + 1:
                    if (
                        len(pending) < process_batch_size
                        and batch_start_block <= end_block
                    ):
                        batch_end_block = min(
                            batch_start_block + request_batch_size - 1, end_block
                        )
                        new_tasks = create_task(
                            progress,
                            task_id,
                            rpc_client,
                            batch_start_block,
                            batch_end_block,
                            entities,
                            exporters,
                        )
                        batch_start_block = batch_start_block + request_batch_size

                        for i in new_tasks:
                            print(i)
                            print(new_tasks[i][2])

                        print()
                        # return
                        pending.update(new_tasks)
                    # return
                    for task_name, task in list(running.items()):
                        if task.done():
                            done[task_name] = True
                            del running[task_name]

                    for name, (func, kwargs, dependencies) in list(pending.items()):
                        if dependencies and not all(
                            dep in done for dep in dependencies
                        ):
                            continue

                        if inspect.iscoroutinefunction(func):
                            coro = func(**kwargs)
                        else:
                            coro = to_thread(func, executor=pool, **kwargs)

                        task = tg.create_task(coro, name=name)
                        running[name] = task
                        del pending[name]

                    await asyncio.sleep(0.1)

    await rpc_client.close()
    # connection manager close here


if __name__ == "__main__":
    args = parse_arg()
    entities = args.entities.split(",") if args.entities else []
    exporters = args.exporters.split(",") if args.exporters else []
    with asyncio.Runner() as runner:
        runner.run(
            main(
                args.start_block,
                args.end_block,
                args.process_batch_size,
                args.request_batch_size,
                entities,
                exporters,
                args.num_workers,
            )
        )

# python -m src.clis.historical --start-block 23170000 --end-block 23170030 \
#     --process-batch-size 100 --request-batch-size 30 \
#     --entities raw_block,block \
#     --exporters sqlite

# python -m src.clis.historical --start-block 23170000 --end-block 23170030 --process-batch-size 100 --request-batch-size 30 --entities raw_block,block,transaction,withdrawal --exporters sqlite
