from __future__ import annotations
from typing import Callable, Dict, Optional, Any

class UdfRegistry:
    _funcs: Dict[str, Callable[..., Any]] = {}

    @classmethod
    def register(cls, name: str, func: Callable[..., Any]) -> None:
        if not name or not callable(func):
            raise ValueError("UDF requires a non-empty name and a callable.")

        cls._funcs[name] = func

    @classmethod
    def get(cls, name: str) -> Optional[Callable[..., Any]]:
        return cls._funcs.get(name)


def register_udf(name: str, func: Callable[..., Any]) -> None:
    UdfRegistry.register(name, func)


def get_udf(name: str) -> Optional[Callable[..., Any]]:
    return UdfRegistry.get(name)
