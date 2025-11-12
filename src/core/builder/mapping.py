from __future__ import annotations
from typing import Any, Dict, Mapping as TMapping, Optional, overload

from ..exceptions import MappingError
from ..engine import DeclarativeConverter
from .rules import Rule, ColumnRuleBuilder, CastType


class MappingBuilder:
    __slots__ = ("_columns", "_explode", "_explode_join", "_definitions", "_schema")

    def __init__(self) -> None:
        self._columns: Dict[str, Any] = {}
        self._explode: Optional[Dict[str, Any]] = None
        self._explode_join: Optional[Dict[str, Any]] = None
        self._definitions: Dict[str, Any] = {}
        self._schema: Optional[Dict[str, Any]] = None

    def explode(self, path: str, *, emit_root_when_empty: bool = True) -> "MappingBuilder":
        if not path or not isinstance(path, str):
            raise ValueError("explode(path=...) requires a non-empty string.")
        self._explode = {"path": path, "emit_root_when_empty": bool(emit_root_when_empty)}
        self._explode_join = None
        return self

    def explode_join(self, left: str, right: str, *, how: str = "inner") -> "MappingBuilder":
        if not left or not right:
            raise ValueError("explode_join(left,right) require non-empty strings.")
        if how not in {"inner", "left", "right", "outer"}:
            raise ValueError("explode_join(how=...) must be one of {'inner','left','right','outer'}.")
        self._explode_join = {"left": left, "right": right, "how": how}
        self._explode = None
        return self

    def col(self, name: str) -> ColumnRuleBuilder:
        if not name or not isinstance(name, str):
            raise ValueError("col(name=...) requires a non-empty string.")
        return ColumnRuleBuilder(self, name)

    @overload
    def set(self, name: str, rule: Dict[str, Any]) -> "MappingBuilder": ...
    @overload
    def set(self, name: str, rule: Rule) -> "MappingBuilder": ...
    @overload
    def set(self, name: str, rule: Any) -> "MappingBuilder": ...

    def set(self, name: str, rule: Any) -> "MappingBuilder":
        if not name or not isinstance(name, str):
            raise ValueError("set(name=...) requires a non-empty string.")
        if isinstance(rule, Rule):
            self._columns[name] = rule.build()
        else:
            self._columns[name] = rule
        return self

    def set_many(self, columns: TMapping[str, Any]) -> "MappingBuilder":
        if not isinstance(columns, dict) or not columns:
            raise ValueError("set_many(columns=...) requires a non-empty dict.")
        for k, v in columns.items():
            self.set(k, v)
        return self

    def col_from_path(self, name: str, dotted: str, *, cast: Optional[CastType] = None, default: Any = None) -> "MappingBuilder":
        rb = Rule().path(dotted)
        if cast:
            rb.cast(cast)
        if default is not None:
            rb.default(default)
        return self.set(name, rb)

    def col_from_rel(self, name: str, dotted: str, *, cast: Optional[CastType] = None, default: Any = None) -> "MappingBuilder":
        rb = Rule().rel_path(dotted)
        if cast:
            rb.cast(cast)
        if default is not None:
            rb.default(default)
        return self.set(name, rb)

    def col_const(self, name: str, value: Any) -> "MappingBuilder":
        return self.set(name, Rule().const(value))

    def define(self, name: str, rule: Rule) -> "MappingBuilder":
        if not name or not isinstance(name, str):
            raise ValueError("define(name=...) requires a non-empty string.")
        self._definitions[name] = rule.build()
        return self

    def with_schema(self, columns_spec: Dict[str, Any], *, strict: bool = False) -> "MappingBuilder":
        self._schema = {"columns": dict(columns_spec), "strict": bool(strict)}
        return self

    def build(self) -> Dict[str, Any]:
        if not self._columns:
            raise MappingError("Cannot build mapping: no columns defined.")
        mapping: Dict[str, Any] = {"columns": {}}
        for k, v in self._columns.items():
            mapping["columns"][k] = v if not isinstance(v, Rule) else v.build()
        if self._explode is not None:
            mapping["explode"] = dict(self._explode)
        if self._explode_join is not None:
            mapping["explode_join"] = dict(self._explode_join)
        if self._definitions:
            mapping["definitions"] = dict(self._definitions)
        if self._schema is not None:
            mapping["schema"] = dict(self._schema)
        DeclarativeConverter._validate_mapping(mapping)
        return mapping

    def to_converter(self) -> DeclarativeConverter:
        return DeclarativeConverter(self.build())

    def from_dict(self, mapping: Dict[str, Any]) -> "MappingBuilder":
        DeclarativeConverter._validate_mapping(mapping)
        self._columns = {k: v for k, v in mapping["columns"].items()}
        self._explode = dict(mapping.get("explode")) if mapping.get("explode") else None
        self._explode_join = dict(mapping.get("explode_join")) if mapping.get("explode_join") else None
        self._definitions = dict(mapping.get("definitions", {}))
        self._schema = dict(mapping.get("schema")) if mapping.get("schema") else None
        return self
