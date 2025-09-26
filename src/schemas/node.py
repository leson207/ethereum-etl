from asyncio import Task
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class Node:
    name: str
    func: Callable
    kwargs: dict
    task: Optional[Task]
    dep_nodes: list[str]
    dep_data: list[str]
    status: str
