from dataclasses import dataclass
from typing import List


@dataclass
class Microtheory:
    isa: List
    gen_ls: List
    genl_mt: List
    disjoint_with: List
    comment: str
    pretty_string: str
    pretty_string_canonical: str
