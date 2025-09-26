import asyncio
import contextvars
import functools
import inspect
from asyncio import TaskGroup
from concurrent.futures import ThreadPoolExecutor

from src.schemas.node import Node


async def to_thread(func, /, *args, executor=None, **kwargs):
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)

    return await loop.run_in_executor(executor, func_call)


class Graph:
    def __init__(self):
        self.nodes: dict[str, Node] = {}

        # self.running_sync_count = 0
        # self.running_async_count = 0
        self.pending_count = 0

    def add_nodes(self, new_nodes):
        self.nodes.update(new_nodes)
        self.pending_count = self.pending_count + len(new_nodes)

    async def run(self, task_group: TaskGroup, thread_pool: ThreadPoolExecutor):
        for name, node in list(self.nodes.items()):
            if node.status == "running" and node.task.done():
                node.status = "done"
                if "finish" in name:
                    for i in node.dep_nodes:
                        del self.nodes[i]

                    del self.nodes[name]

        for name, node in self.nodes.items():
            if node.status != "pending":
                continue

            if not all(
                self.nodes[node_name].status == "done" for node_name in node.dep_nodes
            ):
                continue

            if inspect.iscoroutinefunction(node.func):
                coro = node.func(**node.kwargs)
            else:
                coro = to_thread(node.func, executor=thread_pool, **node.kwargs)

            node.task = task_group.create_task(coro, name=name)
            node.status = "running"
            self.pending_count = self.pending_count - 1
