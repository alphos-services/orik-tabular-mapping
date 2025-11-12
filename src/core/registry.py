from __future__ import annotations
from typing import Any, Callable, Dict, Optional, Set

# Operation handler signature:
# handler(rule: dict,
#         ctx: EvaluationContext,
#         eval_rule: Callable[[Any, Optional[Any]], Any],   # eval_rule(inner, ctx_override=None)
#         apply_tail_ops: Callable[[Any, Dict[str, Any]], Any]
# ) -> Any

OperationHandler = Callable[[Dict[str, Any], Any, Callable[..., Any], Callable[[Any, Dict[str, Any]], Any]], Any]


class OperationRegistry:
    """
    Registry mapping 'head keys' (e.g., 'path', 'math', 'concat', custom ops) to evaluation handlers.
    Handlers return a value (before tail ops).
    """
    def __init__(self) -> None:
        self._handlers: Dict[str, OperationHandler] = {}
        self._order: list[str] = []  # precedence order for detection

    def register(self, head_key: str, handler: OperationHandler, *, prepend: bool = False) -> None:
        if not head_key or not isinstance(head_key, str):
            raise ValueError("head_key must be a non-empty string.")

        self._handlers[head_key] = handler
        if head_key in self._order:
            self._order.remove(head_key)

        if prepend:
            self._order.insert(0, head_key)

        else:
            self._order.append(head_key)

    def get_match_key(self, rule: Dict[str, Any]) -> Optional[str]:
        for k in self._order:
            if k in rule:
                return k

        return None

    def get_handler(self, key: str) -> OperationHandler:
        return self._handlers[key]

    @property
    def head_keys(self) -> Set[str]:
        return set(self._handlers.keys())


_global_registry: Optional[OperationRegistry] = None


def get_registry() -> OperationRegistry:
    global _global_registry
    if _global_registry is None:
        _global_registry = OperationRegistry()
        # Built-ins are registered on import
        from .ops import builtin  # noqa: F401

    return _global_registry


def register_operation(head_key: str, handler: OperationHandler, *, prepend: bool = False) -> None:
    get_registry().register(head_key, handler, prepend=prepend)
