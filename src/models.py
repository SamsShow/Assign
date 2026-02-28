from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


@dataclass
class CompanyRecord:
    id: int
    label: str
    type: str


@dataclass
class PairScore:
    left_id: int
    right_id: int
    score: float
    reason: str
    ai_decision: str | None = None
    ai_same_company: bool | None = None
    ai_canonical_name: str | None = None


class UnionFind:
    def __init__(self) -> None:
        self.parent: Dict[int, int] = {}

    def find(self, item: int) -> int:
        if item not in self.parent:
            self.parent[item] = item
            return item
        if self.parent[item] != item:
            self.parent[item] = self.find(self.parent[item])
        return self.parent[item]

    def union(self, a: int, b: int) -> None:
        root_a = self.find(a)
        root_b = self.find(b)
        if root_a != root_b:
            self.parent[root_b] = root_a

    def groups(self) -> Dict[int, List[int]]:
        result: Dict[int, List[int]] = {}
        for item in list(self.parent.keys()):
            root = self.find(item)
            result.setdefault(root, []).append(item)
        return result
