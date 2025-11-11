from __future__ import annotations
import re
import json
from typing import Any, Dict, List, Union, Optional, Iterable
import pandas as pd

try:
    import torch
except Exception:
    torch = None


Json = Union[Dict[str, Any], List[Any], str, int, float, bool, None]


class MappingError(ValueError):
    pass


class DeclarativeConverter:
    def __init__(self, mapping: Dict[str, Any]) -> None:
        self._validate_mapping(
            mapping=mapping
        )
        self.mapping: Dict[str, Any] = mapping

    @staticmethod
    def _validate_mapping(mapping: Dict[str, Any]) -> None:
        if not isinstance(mapping, dict):
            raise MappingError("Mapping muss ein Objekt sein.")

        cols = mapping.get("columns")
        if not isinstance(cols, dict) or not cols:
            raise MappingError("Mapping muss ein nicht-leeres 'columns'-Objekt enthalten.")

        exp = mapping.get("explode")
        if exp is not None:
            if not isinstance(exp, dict):
                raise MappingError("'explode' muss ein Objekt sein.")

            if "path" not in exp or not isinstance(exp["path"], str) or not exp["path"]:
                raise MappingError("'explode.path' muss ein nicht-leerer String sein.")

            if "emit_root_when_empty" in exp and not isinstance(exp["emit_root_when_empty"], bool):
                raise MappingError("'explode.emit_root_when_empty' muss bool sein, wenn gesetzt.")

    def to_dataframe_single(self, record: Dict[str, Any]) -> pd.DataFrame:
        rows = self._build_rows_for_record(
            rec=record
        )
        return pd.DataFrame(
            data=rows
        )

    def to_dataframe_batch(self, records: Iterable[Dict[str, Any]]) -> pd.DataFrame:
        all_rows: List[Dict[str, Any]] = []
        for rec in records:
            all_rows.extend(
                self._build_rows_for_record(
                    rec=rec
                )
            )

        return pd.DataFrame(all_rows)

    def to_dataframe(self, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> pd.DataFrame:
        if isinstance(data, dict):
            return self.to_dataframe_single(
                record=data
            )

        if isinstance(data, list):
            return self.to_dataframe_batch(
                records=data
            )

        raise TypeError("data muss Dict oder List[Dict] sein.")

    def to_tensor_single(self, record: Dict[str, Any]):
        df = self.to_dataframe_single(
            record=record
        )
        return self._df_to_tensor(
            df=df
        )

    def to_tensor_batch(self, records: Iterable[Dict[str, Any]]):
        df = self.to_dataframe_batch(
            records=records
        )
        return self._df_to_tensor(
            df=df
        )

    def _build_rows_for_record(self, rec: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        explode_spec = self.mapping.get("explode", {})
        explode_path: Optional[str] = explode_spec.get("path")
        emit_root_when_empty: bool = explode_spec.get("emit_root_when_empty", True)

        if explode_path:
            items = self._get_path(
                obj=rec,
                path=explode_path
            )
            if isinstance(items, list) and len(items) > 0:
                for item in items:
                    ctx = {
                        "__root__": rec,
                        "__rel__": item
                    }
                    rows.append(self._eval_columns(ctx=ctx))
            else:
                if emit_root_when_empty:
                    ctx = {
                        "__root__": rec,
                        "__rel__": None
                    }
                    rows.append(self._eval_columns(ctx=ctx))
        else:
            ctx = {
                "__root__": rec,
                "__rel__": None
            }
            rows.append(self._eval_columns(ctx=ctx))

        return rows

    def _eval_columns(self, ctx: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for name, rule in self.mapping["columns"].items():
            out[name] = self._eval_rule(
                rule=rule,
                ctx=ctx
            )
        return out

    def _eval_rule(self, rule: Any, ctx: Dict[str, Any]) -> Any:
        if not isinstance(rule, dict):
            return rule

        if "path" in rule:
            val = self._get_from_root(
                path=rule["path"],
                ctx=ctx
            )
            return self._apply_tail_ops(
                val=val,
                rule=rule
            )

        if "rel_path" in rule:
            val = self._get_from_rel(
                path=rule["rel_path"],
                ctx=ctx
            )
            return self._apply_tail_ops(
                val=val,
                rule=rule
            )

        if "const" in rule:
            val = rule["const"]
            return self._apply_tail_ops(
                val=val,
                rule=rule
            )

        if "coalesce" in rule:
            for candidate in rule["coalesce"]:
                v = self._eval_rule(
                    rule=candidate,
                    ctx=ctx
                )
                if v is not None:
                    return self._apply_tail_ops(
                        val=v,
                        rule=rule
                    )
            return self._apply_tail_ops(
                val=None,
                rule=rule
            )

        if "math" in rule:
            op_and_args = rule["math"]
            if not (isinstance(op_and_args, list) and len(op_and_args) >= 2):
                return self._apply_tail_ops(
                    val=None,
                    rule=rule
                )
            op = op_and_args[0]
            args = [self._to_float(self._eval_rule(rule=a, ctx=ctx)) for a in op_and_args[1:]]
            if any(a is None for a in args):
                val = None
            else:
                if op == "add":
                    val = sum(args)
                elif op == "sub":
                    val = args[0] - sum(args[1:])
                elif op == "mul":
                    val = self._mul_all(
                        nums=args
                    )
                elif op == "div":
                    try:
                        val = args[0]
                        for a in args[1:]:
                            val /= a
                    except ZeroDivisionError:
                        val = None
                else:
                    val = None
            return self._apply_tail_ops(
                val=val,
                rule=rule
            )

        if "concat" in rule:
            parts = [self._eval_rule(rule=p, ctx=ctx) for p in rule["concat"]]
            parts = ["" if p is None else str(p) for p in parts]
            val = "".join(parts)
            return self._apply_tail_ops(val=val, rule=rule)

        if "join" in rule:
            spec = rule["join"]
            items = self._eval_rule(rule=spec.get("over"), ctx=ctx)
            sep = spec.get("sep", ",")
            if isinstance(items, list):
                val = sep.join("" if x is None else str(x) for x in items)
            else:
                val = None
            return self._apply_tail_ops(val, rule)

        if "index" in rule:
            spec = rule["index"]
            arr = self._eval_rule(rule=spec.get("of"), ctx=ctx)
            idx = spec.get("at", 0)
            val = None
            if isinstance(arr, list) and isinstance(idx, int) and 0 <= idx < len(arr):
                val = arr[idx]
            return self._apply_tail_ops(val=val, rule=rule)

        if "len" in rule:
            target = self._eval_rule(rule=rule["len"], ctx=ctx)
            val = len(target) if hasattr(target, "__len__") else None
            return self._apply_tail_ops(val=val, rule=rule)

        if "reduce" in rule:
            spec = rule["reduce"]
            arr = self._eval_rule(rule=spec.get("over"), ctx=ctx)
            op = spec.get("op", "sum")
            val = None
            if isinstance(arr, list):
                nums = [self._to_float(x) for x in arr if self._to_float(x) is not None]
                if len(nums) == 0:
                    val = None
                elif op == "sum":
                    val = float(sum(nums))
                elif op == "mean":
                    val = float(sum(nums) / len(nums))
                elif op == "min":
                    val = float(min(nums))
                elif op == "max":
                    val = float(max(nums))
            return self._apply_tail_ops(val=val, rule=rule)

        if "serialize" in rule:
            spec = rule["serialize"]
            obj = self._eval_rule(rule=spec.get("of"), ctx=ctx)
            try:
                val = json.dumps(obj, ensure_ascii=False)
            except Exception:
                val = None
            return self._apply_tail_ops(val, rule)

        if "date_format" in rule:
            spec = rule["date_format"]
            src = self._eval_rule(rule=spec.get("parse"), ctx=ctx)
            out_fmt = spec.get("fmt", "%Y-%m-%d %H:%M:%S")
            val = self._format_date(src=src, out_fmt=out_fmt, in_fmt=spec.get("fmt_in"))
            return self._apply_tail_ops(val=val, rule=rule)

        if "if" in rule:
            spec = rule["if"]
            cond = self._eval_predicate(pred=spec.get("cond"), ctx=ctx)
            branch = spec.get("then") if cond else spec.get("else")
            val = self._eval_rule(rule=branch, ctx=ctx) if branch is not None else None
            return self._apply_tail_ops(val=val, rule=rule)

        return self._apply_tail_ops(val=None, rule=rule)

    def _eval_predicate(self, pred: Optional[Dict[str, Any]], ctx: Dict[str, Any]) -> bool:
        if not isinstance(pred, dict) or "op" not in pred:
            return False
        op = pred["op"]
        if op == "exists":
            a = self._eval_rule(rule=pred.get("arg"), ctx=ctx)
            return a is not None

        a = self._eval_rule(rule=pred.get("a"), ctx=ctx)
        b = self._eval_rule(rule=pred.get("b"), ctx=ctx)

        if op == "eq":
            return a == b
        if op == "gt":
            af, bf = self._to_float(a), self._to_float(b)
            return af is not None and bf is not None and af > bf
        if op == "lt":
            af, bf = self._to_float(a), self._to_float(b)
            return af is not None and bf is not None and af < bf
        if op == "regex":
            if a is None or b is None:
                return False
            try:
                return re.search(str(b), str(a)) is not None
            except re.error:
                return False
        return False

    def _apply_tail_ops(self, val: Any, rule: Dict[str, Any]) -> Any:
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

    def _get_from_root(self, path: str, ctx: Dict[str, Any]) -> Any:
        return self._get_path(obj=ctx["__root__"], path=path)

    def _get_from_rel(self, path: str, ctx: Dict[str, Any]) -> Any:
        base = ctx.get("__rel__")
        return self._get_path(obj=base, path=path) if base is not None else None

    def _get_path(self, obj: Any, path: Optional[str]) -> Any:
        if path is None:
            return None
        cur = obj
        token_re = re.compile(r"""
            ([^. \[\]]+)      # key
            (?:\[(\d+)])?    # optional [idx]
        """, re.X)
        for part in path.split("."):
            if cur is None:
                return None
            m = token_re.fullmatch(part)
            if not m:
                return None
            key, idx = m.group(1), m.group(2)
            if isinstance(cur, dict):
                cur = cur.get(key)
            else:
                return None
            if idx is not None:
                if isinstance(cur, list):
                    i = int(idx)
                    cur = cur[i] if 0 <= i < len(cur) else None
                else:
                    return None
        return cur

    @staticmethod
    def _mul_all(nums: List[float]) -> float:
        out = 1.0
        for n in nums:
            out *= n
        return out

    @staticmethod
    def _to_float(x: Any) -> Optional[float]:
        if x is None:
            return None
        try:
            return float(x)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _format_date(src: Any, out_fmt: str, in_fmt: Optional[str] = None) -> Optional[str]:
        if src is None:
            return None
        from datetime import datetime
        s = str(src)
        try:
            if in_fmt:
                dt = datetime.strptime(s, in_fmt)
            else:
                try:
                    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                except ValueError:
                    try:
                        _ts = pd.to_datetime(s, utc=False, errors="coerce")
                        if pd.isna(_ts):
                            return None
                        dt = _ts.to_pydatetime()
                    except Exception:
                        return None
            return dt.strftime(out_fmt)
        except Exception:
            return None

    @staticmethod
    def _df_to_tensor(df: pd.DataFrame):
        if torch is None:
            raise RuntimeError("PyTorch ist nicht installiert. Bitte 'pip install torch' ausführen.")
        numeric = df.select_dtypes(include=["number", "bool"]).astype(float)
        return torch.tensor(numeric.values, dtype=torch.float32)
