from dataclasses import dataclass, field
from typing import Callable


@dataclass
class Node:
    name: str
    task: Callable
    kwargs: dict = field(default_factory=dict)
    dep_tasks: list[str] = field(default_factory=list)
    dep_data: list[str] = field(default_factory=list)
