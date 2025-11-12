from __future__ import annotations
from typing import Any, Dict, Optional
from .path import PathResolver


class EvaluationContext:
    """
    Evaluation context providing access to __root__ and __rel__ objects and shared definitions.
    """
    __slots__ = ("root", "rel", "defs")

    def __init__(self, root: Dict[str, Any], rel: Optional[Any], defs: Optional[Dict[str, Any]] = None) -> None:
        self.root = root
        self.rel = rel
        self.defs = defs or {}

    def get_from_root(self, path: str) -> Any:
        return PathResolver.get(self.root, path)

    def get_from_rel(self, path: str) -> Any:
        if self.rel is None:
            return None
        
        return PathResolver.get(self.rel, path)
