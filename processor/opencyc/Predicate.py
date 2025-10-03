from dataclasses import dataclass
from typing import List


@dataclass
class Predicate:
    isa: List
    comment: str
