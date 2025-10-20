import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor

import httpx
import orjson
import uvloop
from httpx_ws import aconnect_ws
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from src.configs.connection_manager import connection_manager
from src.configs.environment import env
from src.logger import logger
from src.tasks.dag import create_node
from src.tasks.graph import Graph

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


def parse_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument("--running-queue-size", type=int, default=10)
    parser.add_argument("--entities", type=str, default=None)
    parser.add_argument("--exporters", type=str, default=None)
    parser.add_argument("--num-workers", type=str, default=4)
    return parser.parse_args()


async def main(
    running_queue_size: int,
    entities: list[str],
    exporters: list[str],
    num_workers: int,
):
    if "pool" in entities:
        await connection_manager.init(exporters + ["rpc", "memgraph"])
    else:
        await connection_manager.init(exporters + ["rpc"])

    graph = Graph(running_queue_size)
    ws_client = httpx.AsyncClient()

    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        task_id = progress.add_task(
            description="Block: ",
            total=1000000,
        )
        with ThreadPoolExecutor(max_workers=num_workers) as pool:
            async with asyncio.TaskGroup() as tg:
                async with aconnect_ws(env.WEBSOCKET_URL, ws_client) as ws:
                    await ws.send_json(
                        {
                            "id": 1,
                            "jsonrpc": "2.0",
                            "method": "eth_subscribe",
                            "params": ["newHeads"],
                        }
                    )
                    msg = await ws.receive_text()
                    msg = orjson.loads(msg)
                    subscription_id = msg["result"]
                    logger.info(f"Subscription ID: {subscription_id}")
                    try:
                        while True:
                            message = await ws.receive_text()
                            message = orjson.loads(message)
                            block_number = int(
                                message["params"]["result"]["number"], 16
                            )

                            logger.info(f"Start process block number: {block_number}")

                            new_nodes = create_node(
                                progress,
                                task_id,
                                connection_manager["rpc"],
                                connection_manager.get("memgraph"),
                                block_number,
                                block_number,
                                entities,
                                exporters,
                            )

                            graph.add_nodes(new_nodes)

                            await graph.run(tg, pool)

                            await asyncio.sleep(0.1)
                    except Exception as e:
                        logger.info(repr(e))
                    finally:
                        await ws.send_json(
                            {
                                "id": 1,
                                "jsonrpc": "2.0",
                                "method": "eth_unsubscribe",
                                "params": [subscription_id],
                            }
                        )

                        await connection_manager.close()


if __name__ == "__main__":
    args = parse_arg()
    entities = args.entities.split(",") if args.entities else []
    exporters = args.exporters.split(",") if args.exporters else []
    with asyncio.Runner() as runner:
        runner.run(
            main(
                args.running_queue_size,
                entities,
                exporters,
                args.num_workers,
            )
        )


# python -m src.clis.realtime_ws --running-queue-size 5 \
# --entities raw_block,block,transaction,withdrawal,raw_receipt,receipt,log,transfer,event,account,pool,token,raw_trace,trace \
# --exporters sqlite
