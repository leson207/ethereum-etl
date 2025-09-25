from dataclasses import dataclass, field
from typing import Callable


@dataclass
class Task:
    name: str
    func: Callable
    args: tuple = field(default_factory=tuple)
    deps: list[str] = field(default_factory=list)
