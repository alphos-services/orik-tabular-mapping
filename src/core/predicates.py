from __future__ import annotations
from typing import Any, Dict, Optional
import re
from .utils import to_float


class Predicate:
    @staticmethod
    def exists(arg: Any) -> Dict[str, Any]:
        return {"op": "exists", "arg": arg}

    @staticmethod
    def eq(a: Any, b: Any) -> Dict[str, Any]:
        return {"op": "eq", "a": a, "b": b}

    @staticmethod
    def gt(a: Any, b: Any) -> Dict[str, Any]:
        return {"op": "gt", "a": a, "b": b}

    @staticmethod
    def lt(a: Any, b: Any) -> Dict[str, Any]:
        return {"op": "lt", "a": a, "b": b}

    @staticmethod
    def regex(a: Any, pattern: Any) -> Dict[str, Any]:
        return {"op": "regex", "a": a, "b": pattern}

    @staticmethod
    def evaluate(pred: Optional[Dict[str, Any]], *, eval_rule) -> bool:
        if not isinstance(pred, dict) or "op" not in pred:
            return False
        op = pred["op"]

        if op == "exists":
            a = eval_rule(pred.get("arg"))
            return a is not None

        a = eval_rule(pred.get("a"))
        b = eval_rule(pred.get("b"))

        if op == "eq":
            return a == b

        if op == "gt":
            af, bf = to_float(a), to_float(b)
            return af is not None and bf is not None and af > bf

        if op == "lt":
            af, bf = to_float(a), to_float(b)
            return af is not None and bf is not None and af < bf

        if op == "regex":
            if a is None or b is None:
                return False

            try:
                return re.search(str(b), str(a)) is not None

            except re.error:
                return False
            
        return False
