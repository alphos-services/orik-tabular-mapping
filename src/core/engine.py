from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Union, Callable, Tuple
import logging
import pandas as pd

from .backends.pandas import DataFrameBackend, PandasBackend
from .exceptions import MappingError
from .context import EvaluationContext
from .registry import get_registry, OperationRegistry
from .utils import df_to_tensor


ErrorMode = str  # "null" | "default" | "raise" | "warn"

@dataclass
class EngineConfig:
    default_on_error: ErrorMode = "null"
    trace_enabled: bool = False
    strict_schema: bool = False
    backend: DataFrameBackend = field(default_factory=PandasBackend)

    logger: Optional[logging.Logger] = None
    metrics_increment: Optional[Callable[[str, int], None]] = None
    metrics_observe: Optional[Callable[[str, float], None]] = None


class DeclarativeConverter:
    """
    Modular, pluggable engine that converts JSON-like records into a tabular structure.
    """
    def __init__(
        self,
        mapping: Dict[str, Any],
        *,
        registry: Optional[OperationRegistry] = None,
        config: Optional[EngineConfig] = None,
        backend: Optional[DataFrameBackend] = None,
    ) -> None:
        self._validate_mapping(mapping)
        self.mapping: Dict[str, Any] = mapping

        self._definitions: Dict[str, Any] = mapping.get("definitions", {}) or {}
        self._schema: Optional[Dict[str, Any]] = mapping.get("schema")

        self._registry: OperationRegistry = registry or get_registry()
        self._config: EngineConfig = config or EngineConfig()
        if backend is not None:
            self._config.backend = backend

        self._validate_definitions(self._definitions)
        self._validate_schema(self._schema)

        self._trace_enabled = bool(self._config.trace_enabled)

    def to_dataframe_single(self, record: Dict[str, Any]):
        rows = self._build_rows_for_record(record)
        df = self._config.backend.to_dataframe(rows)
        self._apply_output_schema_if_any(df)
        return df

    def to_dataframe_batch(self, records: Iterable[Dict[str, Any]]):
        frames: List[Any] = []
        for rec in records:
            rows = self._build_rows_for_record(rec)
            frames.append(self._config.backend.to_dataframe(rows))

        df = self._config.backend.concat(frames)
        self._apply_output_schema_if_any(df)
        return df

    def to_dataframe(self, data: Union[Dict[str, Any], List[Dict[str, Any]]]):
        if isinstance(data, dict):
            return self.to_dataframe_single(data)

        if isinstance(data, list):
            return self.to_dataframe_batch(data)

        raise TypeError("data must be Dict or List[Dict].")

    def to_tensor_single(self, record: Dict[str, Any]):
        df = self.to_dataframe_single(record)
        return df_to_tensor(df)

    def to_tensor_batch(self, records: Iterable[Dict[str, Any]]):
        df = self.to_dataframe_batch(records)
        return df_to_tensor(df)

    def trace(self, record: Dict[str, Any]) -> Dict[str, Any]:
        prev = self._trace_enabled
        try:
            self._trace_enabled = True
            ctx_rows, traces = self._build_rows_with_trace(record)
            return {"rows_emitted": len(ctx_rows), "columns_trace": traces}

        finally:
            self._trace_enabled = prev

    @staticmethod
    def _validate_mapping(mapping: Dict[str, Any]) -> None:
        if not isinstance(mapping, dict):
            raise MappingError("Mapping must be an object (dict).")

        cols = mapping.get("columns")
        if not isinstance(cols, dict) or not cols:
            raise MappingError("Mapping must contain a non-empty 'columns' object.")

        exp = mapping.get("explode")
        if exp is not None:
            if not isinstance(exp, dict):
                raise MappingError("'explode' must be an object.")

            if "path" not in exp or not isinstance(exp["path"], str) or not exp["path"]:
                raise MappingError("'explode.path' must be a non-empty string.")

            if "emit_root_when_empty" in exp and not isinstance(exp["emit_root_when_empty"], bool):
                raise MappingError("'explode.emit_root_when_empty' must be a boolean if set.")

        expj = mapping.get("explode_join")
        if expj is not None:
            if not isinstance(expj, dict):
                raise MappingError("'explode_join' must be an object.")

            if not isinstance(expj.get("left"), str) or not isinstance(expj.get("right"), str):
                raise MappingError("'explode_join.left' and 'right' must be non-empty strings.")

            if "how" in expj and expj["how"] not in {"inner", "left", "right", "outer"}:
                raise MappingError("'explode_join.how' must be one of {'inner','left','right','outer'}.")

        if "definitions" in mapping and not isinstance(mapping["definitions"], dict):
            raise MappingError("'definitions' must be an object (dict) if present.")

        if "schema" in mapping and not isinstance(mapping["schema"], dict):
            raise MappingError("'schema' must be an object (dict) if present.")

    @staticmethod
    def _validate_definitions(defs: Dict[str, Any]) -> None:
        for k, v in defs.items():
            if not isinstance(k, str) or not k:
                raise MappingError("definitions keys must be non-empty strings.")

            if not isinstance(v, (dict, list, str, int, float, bool, type(None))):
                raise MappingError(f"definition '{k}' has an unsupported value type.")

    @staticmethod
    def _validate_schema(schema: Optional[Dict[str, Any]]) -> None:
        if not schema:
            return

        cols = schema.get("columns")
        if cols is not None and not isinstance(cols, dict):
            raise MappingError("'schema.columns' must be an object (dict) if present.")

        if "strict" in schema and not isinstance(schema["strict"], bool):
            raise MappingError("'schema.strict' must be a boolean when provided.")

    def _build_rows_for_record(self, rec: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []

        expj = self.mapping.get("explode_join")
        if expj:
            left = EvaluationContext(rec, None, self._definitions).get_from_root(expj["left"])
            right = EvaluationContext(rec, None, self._definitions).get_from_root(expj["right"])
            how = expj.get("how", "inner")

            left_list = left if isinstance(left, list) else []
            right_list = right if isinstance(right, list) else []

            def emit(l, r):
                ctx = EvaluationContext(rec, {"left": l, "right": r}, self._definitions)
                rows.append(self._eval_columns(ctx))

            if how == "inner":
                if left_list and right_list:
                    for l in left_list:
                        for r in right_list:
                            emit(l, r)

            elif how == "left":
                if left_list:
                    if right_list:
                        for l in left_list:
                            for r in right_list:
                                emit(l, r)

                    else:
                        for l in left_list:
                            emit(l, None)

            elif how == "right":
                if right_list:
                    if left_list:
                        for r in right_list:
                            for l in left_list:
                                emit(l, r)

                    else:
                        for r in right_list:
                            emit(None, r)

            else:  # outer
                if left_list and right_list:
                    for l in left_list:
                        for r in right_list:
                            emit(l, r)

                elif left_list:
                    for l in left_list:
                        emit(l, None)

                elif right_list:
                    for r in right_list:
                        emit(None, r)

                else:
                    ctx = EvaluationContext(rec, {"left": None, "right": None}, self._definitions)
                    rows.append(self._eval_columns(ctx))
            return rows

        explode_spec = self.mapping.get("explode", {})
        explode_path: Optional[str] = explode_spec.get("path")
        emit_root_when_empty: bool = explode_spec.get("emit_root_when_empty", True)

        if explode_path:
            items = EvaluationContext(rec, None, self._definitions).get_from_root(explode_path)
            if isinstance(items, list) and len(items) > 0:
                for item in items:
                    ctx = EvaluationContext(rec, item, self._definitions)
                    rows.append(self._eval_columns(ctx))
            else:
                if emit_root_when_empty:
                    ctx = EvaluationContext(rec, None, self._definitions)
                    rows.append(self._eval_columns(ctx))
        else:
            ctx = EvaluationContext(rec, None, self._definitions)
            rows.append(self._eval_columns(ctx))

        return rows

    def _build_rows_with_trace(self, rec: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[int, Dict[str, Any]]]:
        traces: Dict[int, Dict[str, Any]] = {}
        rows: List[Dict[str, Any]] = []
        row_idx = 0

        expj = self.mapping.get("explode_join")
        if expj:
            left = EvaluationContext(rec, None, self._definitions).get_from_root(expj["left"])
            right = EvaluationContext(rec, None, self._definitions).get_from_root(expj["right"])
            how = expj.get("how", "inner")

            left_list = left if isinstance(left, list) else []
            right_list = right if isinstance(right, list) else []

            def emit(l, r):
                nonlocal row_idx
                ctx = EvaluationContext(rec, {"left": l, "right": r}, self._definitions)
                out, t = self._eval_columns_with_trace(ctx)
                rows.append(out)
                traces[row_idx] = t
                row_idx += 1

            if how == "inner":
                if left_list and right_list:
                    for l in left_list:
                        for r in right_list:
                            emit(l, r)

            elif how == "left":
                if left_list:
                    if right_list:
                        for l in left_list:
                            for r in right_list:
                                emit(l, r)

                    else:
                        for l in left_list:
                            emit(l, None)

            elif how == "right":
                if right_list:
                    if left_list:
                        for r in right_list:
                            for l in left_list:
                                emit(l, r)

                    else:
                        for r in right_list:
                            emit(None, r)

            else:  # outer
                if left_list and right_list:
                    for l in left_list:
                        for r in right_list:
                            emit(l, r)

                elif left_list:
                    for l in left_list:
                        emit(l, None)

                elif right_list:
                    for r in right_list:
                        emit(None, r)

                else:
                    ctx = EvaluationContext(rec, {"left": None, "right": None}, self._definitions)
                    out, t = self._eval_columns_with_trace(ctx)
                    rows.append(out)
                    traces[row_idx] = t
            return rows, traces

        explode_spec = self.mapping.get("explode", {})
        explode_path: Optional[str] = explode_spec.get("path")
        emit_root_when_empty: bool = explode_spec.get("emit_root_when_empty", True)

        if explode_path:
            items = EvaluationContext(rec, None, self._definitions).get_from_root(explode_path)
            if isinstance(items, list) and len(items) > 0:
                for item in items:
                    ctx = EvaluationContext(rec, item, self._definitions)
                    out, t = self._eval_columns_with_trace(ctx)
                    rows.append(out)
                    traces[row_idx] = t
                    row_idx += 1
            else:
                if emit_root_when_empty:
                    ctx = EvaluationContext(rec, None, self._definitions)
                    out, t = self._eval_columns_with_trace(ctx)
                    rows.append(out)
                    traces[row_idx] = t
        else:
            ctx = EvaluationContext(rec, None, self._definitions)
            out, t = self._eval_columns_with_trace(ctx)
            rows.append(out)
            traces[row_idx] = t

        return rows, traces

    def _eval_columns(self, ctx: EvaluationContext) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for name, rule in self.mapping["columns"].items():
            out[name] = self._eval_rule(rule, ctx, col=name)
        return out

    def _eval_columns_with_trace(self, ctx: EvaluationContext) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        out: Dict[str, Any] = {}
        trace: Dict[str, Any] = {}
        for name, rule in self.mapping["columns"].items():
            val, t = self._eval_rule_with_trace(rule, ctx, col=name)
            out[name] = val
            trace[name] = t
        return out, trace

    def _eval_rule(self, rule: Any, ctx: EvaluationContext, *, col: Optional[str] = None) -> Any:
        if not isinstance(rule, dict):
            return rule

        head_key = self._registry.get_match_key(rule)
        if head_key is None:
            return self._apply_tail_ops(None, rule)

        handler = self._registry.get_handler(head_key)

        def _eval(inner, ctx_override: Optional[EvaluationContext] = None):
            return self._eval_rule(inner, ctx_override or ctx, col=col)

        try:
            return handler(rule, ctx, _eval, self._apply_tail_ops)
        except Exception as exc:
            return self._handle_rule_error(rule, exc, col=col)

    def _eval_rule_with_trace(self, rule: Any, ctx: EvaluationContext, *, col: Optional[str] = None) -> Tuple[Any, Any]:
        if not isinstance(rule, dict):
            return rule, {"literal": rule}

        head_key = self._registry.get_match_key(rule)
        if head_key is None:
            val = self._apply_tail_ops(None, rule)
            return val, {"op": None, "value": val, "note": "no-op; tail-only"}

        handler = self._registry.get_handler(head_key)
        child_traces: List[Any] = []

        def _eval(inner, ctx_override: Optional[EvaluationContext] = None):
            val, t = self._eval_rule_with_trace(inner, ctx_override or ctx, col=col)
            child_traces.append(t)
            return val

        try:
            val = handler(rule, ctx, _eval, self._apply_tail_ops)
            node = {"op": head_key, "rule": rule, "children": child_traces, "value": val}
            return val, node
        except Exception as exc:
            val = self._handle_rule_error(rule, exc, col=col)
            node = {"op": head_key, "rule": rule, "children": child_traces, "error": repr(exc), "value": val}
            return val, node

    @staticmethod
    def _apply_tail_ops(val: Any, rule: Dict[str, Any]) -> Any:
        if val is None and "default" in rule:
            val = rule["default"]

        if "cast" in rule and val is not None:
            t = rule["cast"]
            try:
                if t == "str":
                    val = str(val)

                elif t == "int":
                    val = int(float(val))

                elif t == "float":
                    val = float(val)

                elif t == "bool":
                    val = bool(val)

            except (ValueError, TypeError):
                val = None

        return val

    def _handle_rule_error(self, rule: Dict[str, Any], exc: Exception, *, col: Optional[str]) -> Any:
        mode: ErrorMode = rule.get("on_error") or self._config.default_on_error or "null"

        if self._config.metrics_increment:
            self._config.metrics_increment("converter.rule_errors", 1)

        if mode == "raise":
            raise

        if mode == "warn":
            logger = self._config.logger
            if logger is not None:
                col_info = f" for column '{col}'" if col else ""
                logger.warning("Rule error%s: %s | rule=%s", col_info, repr(exc), rule)

            return None
        if mode == "default":
            return rule.get("default")

        return None

    def _apply_output_schema_if_any(self, df: Any) -> None:
        if not self._schema:
            return

        report = self._validate_output_schema(df, self._schema)
        strict = bool(self._schema.get("strict", self._config.strict_schema))
        if strict and (report.get("errors") or report.get("violations")):
            raise MappingError(f"Output schema validation failed: {report}")

        if self._config.logger:
            self._config.logger.info("Schema validation report: %s", report)

    @staticmethod
    def _validate_output_schema(df: Any, schema: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {"errors": [], "violations": []}
        cols_spec: Dict[str, Any] = (schema.get("columns") or {})
        if not cols_spec:
            return out
        if not isinstance(df, pd.DataFrame):
            return out

        import re as _re
        type_map = {"str": "object", "int": "int", "float": "float", "bool": "bool", "number": ("int", "float", "bool")}

        for col, spec in cols_spec.items():
            if col not in df.columns:
                out["errors"].append({"column": col, "error": "missing"})
                continue

            series = df[col]
            expected = spec.get("type")
            if expected:
                pandas_ok = False
                if expected == "str":
                    pandas_ok = series.dtype == "object" or pd.api.types.is_string_dtype(series)

                elif expected == "int":
                    pandas_ok = pd.api.types.is_integer_dtype(series)

                elif expected == "float":
                    pandas_ok = pd.api.types.is_float_dtype(series)

                elif expected == "bool":
                    pandas_ok = pd.api.types.is_bool_dtype(series)

                elif expected == "number":
                    pandas_ok = pd.api.types.is_numeric_dtype(series) or pd.api.types.is_bool_dtype(series)

                else:
                    out["violations"].append({"column": col, "type": "unknown-type", "expected": expected})

                if not pandas_ok:
                    out["violations"].append({"column": col, "type": "dtype-mismatch", "expected": expected, "actual": str(series.dtype)})

            nullable = spec.get("nullable")
            if nullable is False and series.isna().any():
                out["violations"].append({"column": col, "type": "null-not-allowed", "count": int(series.isna().sum())})

            if pd.api.types.is_numeric_dtype(series):
                if "min" in spec:
                    below = series[series < spec["min"]].shape[0]
                    if below:
                        out["violations"].append({"column": col, "type": "min-violation", "count": int(below), "min": spec["min"]})

                if "max" in spec:
                    above = series[series > spec["max"]].shape[0]
                    if above:
                        out["violations"].append({"column": col, "type": "max-violation", "count": int(above), "max": spec["max"]})

            regex = spec.get("regex")
            if regex:
                try:
                    pat = _re.compile(regex)
                    bad = series.fillna("").astype(str).apply(lambda s: pat.search(s) is None).sum()
                    if bad:
                        out["violations"].append({"column": col, "type": "regex-violation", "count": int(bad), "pattern": regex})

                except Exception as e:
                    out["errors"].append({"column": col, "error": f"invalid-regex: {e}"})

        return out
