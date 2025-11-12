from __future__ import annotations
from typing import Any, Dict, Optional, overload

try:
    from typing import Literal
except Exception:
    Literal = str  # type: ignore

CastType  = Literal["str", "int", "float", "bool"]
ErrorMode = Literal["null", "default", "raise", "warn"]


def _deepcopy_rule(obj: Any) -> Any:
    if isinstance(obj, (dict, list)):
        import copy
        return copy.deepcopy(obj)
    return obj


class PredicateBuilder:
    __slots__ = ("_pred",)

    def __init__(self, pred: Dict[str, Any]) -> None:
        self._pred = pred

    @staticmethod
    def exists(arg: Any) -> "PredicateBuilder":
        return PredicateBuilder({"op": "exists", "arg": _RuleBuilder._as_rule(arg)})

    @staticmethod
    def eq(a: Any, b: Any) -> "PredicateBuilder":
        return PredicateBuilder({"op": "eq", "a": _RuleBuilder._as_rule(a), "b": _RuleBuilder._as_rule(b)})

    @staticmethod
    def gt(a: Any, b: Any) -> "PredicateBuilder":
        return PredicateBuilder({"op": "gt", "a": _RuleBuilder._as_rule(a), "b": _RuleBuilder._as_rule(b)})

    @staticmethod
    def lt(a: Any, b: Any) -> "PredicateBuilder":
        return PredicateBuilder({"op": "lt", "a": _RuleBuilder._as_rule(a), "b": _RuleBuilder._as_rule(b)})

    @staticmethod
    def regex(a: Any, pattern: Any) -> "PredicateBuilder":
        return PredicateBuilder({"op": "regex", "a": _RuleBuilder._as_rule(a), "b": _RuleBuilder._as_rule(pattern)})

    def build(self) -> Dict[str, Any]:
        return _deepcopy_rule(self._pred)


class _RuleBuilder:
    __slots__ = ("_rule",)

    def __init__(self, initial: Optional[Dict[str, Any]] = None) -> None:
        self._rule: Dict[str, Any] = initial or {}

    @staticmethod
    def _as_rule(x: Any) -> Any:
        if isinstance(x, _RuleBuilder):
            return x.build()
        if isinstance(x, PredicateBuilder):
            return x.build()
        return _deepcopy_rule(x)

    def _merge(self, fragment: Dict[str, Any]) -> "_RuleBuilder":
        self._rule.update(fragment)
        return self

    def path(self, dotted: str) -> "_RuleBuilder":
        if not dotted or not isinstance(dotted, str):
            raise ValueError("path() requires a non-empty string.")
        return self._merge({"path": dotted})

    def rel_path(self, dotted: str) -> "_RuleBuilder":
        if not dotted or not isinstance(dotted, str):
            raise ValueError("rel_path() requires a non-empty string.")
        return self._merge({"rel_path": dotted})

    def const(self, value: Any) -> "_RuleBuilder":
        return self._merge({"const": _deepcopy_rule(value)})

    def coalesce(self, *candidates: Any) -> "_RuleBuilder":
        if not candidates:
            raise ValueError("coalesce() needs at least one candidate.")
        return self._merge({"coalesce": [self._as_rule(c) for c in candidates]})

    def _math(self, op: str, *args: Any) -> "_RuleBuilder":
        if len(args) < 1:
            raise ValueError(f"{op}() requires at least one argument.")
        return self._merge({"math": [op] + [self._as_rule(a) for a in args]})

    def add(self, *args: Any) -> "_RuleBuilder": return self._math("add", *args)
    def sub(self, *args: Any) -> "_RuleBuilder": return self._math("sub", *args)
    def mul(self, *args: Any) -> "_RuleBuilder": return self._math("mul", *args)
    def div(self, *args: Any) -> "_RuleBuilder": return self._math("div", *args)

    def concat(self, *parts: Any) -> "_RuleBuilder":
        if not parts:
            raise ValueError("concat() needs at least one part.")
        return self._merge({"concat": [self._as_rule(p) for p in parts]})

    def join(self, over: Any, sep: str = ",") -> "_RuleBuilder":
        if not isinstance(sep, str):
            raise ValueError("join(sep=...) must be a string.")
        return self._merge({"join": {"over": self._as_rule(over), "sep": sep}})

    def index(self, of: Any, at: int = 0) -> "_RuleBuilder":
        if not isinstance(at, int) or at < 0:
            raise ValueError("index(at=...) must be a non-negative integer.")
        return self._merge({"index": {"of": self._as_rule(of), "at": at}})

    def length(self, target: Any) -> "_RuleBuilder":
        return self._merge({"len": self._as_rule(target)})

    def reduce(self, over: Any, op: str = "sum", apply: Any = None) -> "_RuleBuilder":
        if op not in {"sum", "mean", "min", "max"}:
            raise ValueError("reduce(op=...) must be one of {'sum','mean','min','max'}.")
        spec: Dict[str, Any] = {"over": self._as_rule(over), "op": op}
        if apply is not None:
            spec["apply"] = self._as_rule(apply)
        return self._merge({"reduce": spec})

    def serialize(self, of: Any) -> "_RuleBuilder":
        return self._merge({"serialize": {"of": self._as_rule(of)}})

    def date_format(self, parse: Any, fmt: str = "%Y-%m-%d %H:%M:%S", fmt_in: Optional[str] = None) -> "_RuleBuilder":
        spec: Dict[str, Any] = {"parse": self._as_rule(parse), "fmt": fmt}
        if fmt_in is not None:
            spec["fmt_in"] = fmt_in
        return self._merge({"date_format": spec})

    def date_parse(self, text: Any, formats: Optional[list[str]] = None, strict: bool = False) -> "_RuleBuilder":
        return self._merge({"date_parse": {"text": self._as_rule(text), "formats": formats or [], "strict": strict}})

    def to_timezone(self, dt: Any, to: str, from_tz: Optional[str] = None) -> "_RuleBuilder":
        spec = {"dt": self._as_rule(dt), "to": to}
        if from_tz:
            spec["from"] = from_tz
        return self._merge({"to_timezone": spec})

    def from_timestamp(self, sec: Any, unit: str = "s") -> "_RuleBuilder":
        return self._merge({"from_timestamp": {"sec": self._as_rule(sec), "unit": unit}})

    def when(self, cond: "PredicateBuilder", then: Any, otherwise: Any = None) -> "_RuleBuilder":
        if not isinstance(cond, PredicateBuilder):
            raise TypeError("when(cond=...) requires a PredicateBuilder.")
        return self._merge({"if": {"cond": cond.build(), "then": self._as_rule(then), "else": self._as_rule(otherwise)}})

    def map(self, over: Any, apply: Any, *, emit: Optional[str] = None, sep: str = ",", limit: Optional[int] = None) -> "_RuleBuilder":
        spec: Dict[str, Any] = {"over": self._as_rule(over), "apply": self._as_rule(apply)}
        if emit is not None:
            spec["emit"] = emit
        if limit is not None:
            spec["limit"] = int(limit)
        if emit == "join":
            spec["sep"] = sep
        return self._merge({"map": spec})

    def filter(self, over: Any, where: "PredicateBuilder", *, emit: Optional[str] = None, sep: str = ",", limit: Optional[int] = None) -> "_RuleBuilder":
        if not isinstance(where, PredicateBuilder):
            raise TypeError("filter(where=...) requires a PredicateBuilder.")
        spec: Dict[str, Any] = {"over": self._as_rule(over), "where": where.build()}
        if emit is not None:
            spec["emit"] = emit
        if limit is not None:
            spec["limit"] = int(limit)
        if emit == "join":
            spec["sep"] = sep
        return self._merge({"filter": spec})

    def flat_map(self, over: Any, apply: Any, *, emit: Optional[str] = None, sep: str = ",", limit: Optional[int] = None) -> "_RuleBuilder":
        spec: Dict[str, Any] = {"over": self._as_rule(over), "apply": self._as_rule(apply)}
        if emit is not None:
            spec["emit"] = emit
        if limit is not None:
            spec["limit"] = int(limit)
        if emit == "join":
            spec["sep"] = sep
        return self._merge({"flat_map": spec})

    def unique(self, over: Any, key: Optional[Any] = None, *, emit: Optional[str] = None, sep: str = ",", limit: Optional[int] = None) -> "_RuleBuilder":
        spec: Dict[str, Any] = {"over": self._as_rule(over)}
        if key is not None:
            spec["key"] = self._as_rule(key)
        if emit is not None:
            spec["emit"] = emit
        if limit is not None:
            spec["limit"] = int(limit)
        if emit == "join":
            spec["sep"] = sep
        return self._merge({"unique": spec})

    def sort(self, over: Any, key: Optional[Any] = None, reverse: bool = False, *, emit: Optional[str] = None, sep: str = ",", limit: Optional[int] = None) -> "_RuleBuilder":
        spec: Dict[str, Any] = {"over": self._as_rule(over), "reverse": reverse}
        if key is not None:
            spec["key"] = self._as_rule(key)
        if emit is not None:
            spec["emit"] = emit
        if limit is not None:
            spec["limit"] = int(limit)
        if emit == "join":
            spec["sep"] = sep
        return self._merge({"sort": spec})

    def lookup(self, key: Any, table: Dict[str, Any], default: Any = None) -> "_RuleBuilder":
        return self._merge({"lookup": {"key": self._as_rule(key), "table": _deepcopy_rule(table), "default": _deepcopy_rule(default)}})

    def merge(self, *objects: Any, strategy: str = "override") -> "_RuleBuilder":
        return self._merge({"merge": {"objects": [self._as_rule(o) for o in objects], "strategy": strategy}})

    def ref(self, name: str) -> "_RuleBuilder":
        return self._merge({"ref": name})

    def udf(self, name: str, *args: Any) -> "_RuleBuilder":
        return self._merge({"udf": {"name": name, "args": [self._as_rule(a) for a in args]}})

    def default(self, value: Any) -> "_RuleBuilder":
        self._rule["default"] = _deepcopy_rule(value)
        return self

    def cast(self, to: CastType) -> "_RuleBuilder":
        allowed = {"str", "int", "float", "bool"}
        if to not in allowed:
            raise ValueError(f"cast(to=...) must be one of {allowed}.")
        self._rule["cast"] = to  # type: ignore[assignment]
        return self

    def on_error(self, mode: ErrorMode) -> "_RuleBuilder":
        allowed = {"null", "default", "raise", "warn"}
        if mode not in allowed:
            raise ValueError(f"on_error(mode=...) must be one of {allowed}.")
        self._rule["on_error"] = mode  # type: ignore[assignment]
        return self

    def build(self) -> Dict[str, Any]:
        return _deepcopy_rule(self._rule)


class Rule(_RuleBuilder):
    __slots__ = ()


class ColumnRuleBuilder(_RuleBuilder):
    __slots__ = ("_parent", "_name")

    def __init__(self, parent: "MappingBuilder", name: str) -> None:
        super().__init__()
        self._parent = parent
        self._name = name

    def end(self) -> "MappingBuilder":
        self._parent._columns[self._name] = self.build()
        return self._parent

    done = end
