from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

from ..registry import register_operation
from ..utils import to_float, mul_all, format_date
from ..predicates import Predicate
from ..context import EvaluationContext


def _ensure_list(x: Any) -> List[Any]:
    if isinstance(x, list):
        return x
    return []

def _to_str_for_join(x: Any) -> str:
    if isinstance(x, (dict, list)):
        try:
            return json.dumps(x, ensure_ascii=False)
        except Exception:
            return str(x)
    return "" if x is None else str(x)

def _emit_list_like(out: List[Any], spec: Dict[str, Any], rule: Dict[str, Any], apply_tail_ops):
    limit = spec.get("limit")
    if isinstance(limit, int) and limit >= 0:
        out = out[:limit]

    emit = spec.get("emit") or spec.get("as")
    if emit is None or emit == "list":
        return apply_tail_ops(out, rule)
    if emit == "json":
        try:
            return apply_tail_ops(json.dumps(out, ensure_ascii=False), rule)
        except Exception:
            return apply_tail_ops(None, rule)
    if emit == "count":
        return apply_tail_ops(len(out), rule)
    if emit == "join":
        sep = spec.get("sep", ",")
        return apply_tail_ops(sep.join(_to_str_for_join(x) for x in out), rule)
    # Fallback
    return apply_tail_ops(out, rule)


def _op_path(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    val = ctx.get_from_root(rule["path"])
    return apply_tail_ops(val, rule)
register_operation("path", _op_path)

def _op_rel_path(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    val = ctx.get_from_rel(rule["rel_path"])
    return apply_tail_ops(val, rule)
register_operation("rel_path", _op_rel_path)

def _op_const(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    return apply_tail_ops(rule.get("const"), rule)
register_operation("const", _op_const)

def _op_coalesce(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    for candidate in rule.get("coalesce", []):
        v = eval_rule(candidate)
        if v is not None:
            return apply_tail_ops(v, rule)
    return apply_tail_ops(None, rule)
register_operation("coalesce", _op_coalesce)


def _op_math(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    spec = rule["math"]
    if not (isinstance(spec, list) and len(spec) >= 2):
        return apply_tail_ops(None, rule)
    op = spec[0]
    args = [to_float(eval_rule(a)) for a in spec[1:]]
    if any(a is None for a in args):
        val = None
    else:
        if op == "add":
            val = sum(args)
        elif op == "sub":
            val = args[0] - sum(args[1:])
        elif op == "mul":
            val = mul_all(args)
        elif op == "div":
            try:
                val = args[0]
                for a in args[1:]:
                    val /= a
            except ZeroDivisionError:
                val = None
        else:
            val = None
    return apply_tail_ops(val, rule)
register_operation("math", _op_math)


def _op_concat(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    parts = [eval_rule(p) for p in rule["concat"]]
    parts = ["" if p is None else str(p) for p in parts]
    val = "".join(parts)
    return apply_tail_ops(val, rule)
register_operation("concat", _op_concat)

def _op_join(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    spec = rule["join"]
    items = eval_rule(spec.get("over"))
    sep = spec.get("sep", ",")
    if isinstance(items, list):
        val = sep.join("" if x is None else str(x) for x in items)
    else:
        val = None
    return apply_tail_ops(val, rule)
register_operation("join", _op_join)

def _op_index(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    spec = rule["index"]
    arr = eval_rule(spec.get("of"))
    idx = spec.get("at", 0)
    val = None
    if isinstance(arr, list) and isinstance(idx, int) and 0 <= idx < len(arr):
        val = arr[idx]
    return apply_tail_ops(val, rule)
register_operation("index", _op_index)

def _op_len(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    target = eval_rule(rule["len"])
    val = len(target) if hasattr(target, "__len__") else None
    return apply_tail_ops(val, rule)
register_operation("len", _op_len)

def _op_reduce(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    """
    Enhanced reduce: supports optional 'apply' to project list elements.
      {"reduce": {"over": <rule>, "apply": <rule?>, "op": "sum|mean|min|max"}}
    """
    spec = rule["reduce"]
    over = eval_rule(spec.get("over"))
    op = spec.get("op", "sum")
    apply_rule = spec.get("apply")

    vals: List[float] = []
    if isinstance(over, list):
        if apply_rule is not None:
            for el in over:
                subctx = type(ctx)(ctx.root, el, ctx.defs) if isinstance(ctx, EvaluationContext) else ctx
                v = eval_rule(apply_rule, ctx_override=subctx)
                fv = to_float(v)
                if fv is not None:
                    vals.append(fv)
        else:
            for x in over:
                fx = to_float(x)
                if fx is not None:
                    vals.append(fx)
    else:
        vals = []

    if not vals:
        return apply_tail_ops(None, rule)

    if op == "sum":
        val = float(sum(vals))
    elif op == "mean":
        val = float(sum(vals) / len(vals))
    elif op == "min":
        val = float(min(vals))
    elif op == "max":
        val = float(max(vals))
    else:
        val = None
    return apply_tail_ops(val, rule)
register_operation("reduce", _op_reduce)


def _op_date_format(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    spec = rule["date_format"]
    src = eval_rule(spec.get("parse"))
    out_fmt = spec.get("fmt", "%Y-%m-%d %H:%M:%S")
    val = format_date(src=src, out_fmt=out_fmt, in_fmt=spec.get("fmt_in"))
    return apply_tail_ops(val, rule)
register_operation("date_format", _op_date_format)

def _op_date_parse(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    spec = rule["date_parse"]
    text = eval_rule(spec.get("text"))
    formats = spec.get("formats") or []
    strict = bool(spec.get("strict", False))
    if text is None:
        return apply_tail_ops(None, rule)
    s = str(text)
    dt = None
    if formats:
        for f in formats:
            try:
                dt = datetime.strptime(s, f)
                break
            except Exception:
                continue
    if dt is None and not strict:
        try:
            from pandas import to_datetime
            ts = to_datetime(s, utc=False, errors="coerce")
            dt = None if ts is None or str(ts) == "NaT" else ts.to_pydatetime()
        except Exception:
            dt = None
    return apply_tail_ops(dt.isoformat() if dt else None, rule)
register_operation("date_parse", _op_date_parse)

def _op_from_timestamp(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    spec = rule["from_timestamp"]
    sec = eval_rule(spec.get("sec"))
    unit = spec.get("unit", "s")
    if sec is None:
        return apply_tail_ops(None, rule)
    try:
        val = float(sec)
        if unit == "ms":
            val /= 1000.0
        elif unit == "us":
            val /= 1_000_000.0
        elif unit == "ns":
            val /= 1_000_000_000.0
        dt = datetime.fromtimestamp(val, tz=timezone.utc)
        return apply_tail_ops(dt.isoformat(), rule)
    except Exception:
        return apply_tail_ops(None, rule)
register_operation("from_timestamp", _op_from_timestamp)

def _op_to_timezone(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    spec = rule["to_timezone"]
    dt_iso = eval_rule(spec.get("dt"))
    from_tz = spec.get("from")
    to_tz = spec.get("to")
    if dt_iso is None or to_tz is None:
        return apply_tail_ops(None, rule)
    try:
        dt = datetime.fromisoformat(str(dt_iso).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            if from_tz and ZoneInfo:
                dt = dt.replace(tzinfo=ZoneInfo(from_tz))
            else:
                dt = dt.replace(tzinfo=timezone.utc)
        if ZoneInfo:
            dt2 = dt.astimezone(ZoneInfo(to_tz))
        else:
            dt2 = dt.astimezone(timezone.utc)
        return apply_tail_ops(dt2.isoformat(), rule)
    except Exception:
        return apply_tail_ops(None, rule)
register_operation("to_timezone", _op_to_timezone)


def _op_if(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    spec = rule["if"]
    cond = Predicate.evaluate(spec.get("cond"), eval_rule=eval_rule)
    branch = spec.get("then") if cond else spec.get("else")
    val = eval_rule(branch) if branch is not None else None
    return apply_tail_ops(val, rule)
register_operation("if", _op_if)


def _op_ref(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    name = rule["ref"]
    frag = ctx.defs.get(name)
    val = eval_rule(frag) if frag is not None else None
    return apply_tail_ops(val, rule)
register_operation("ref", _op_ref)


def _op_lookup(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    spec = rule["lookup"]
    key = eval_rule(spec.get("key"))
    table = spec.get("table", {})
    if isinstance(table, dict) and key is not None:
        val = table.get(key, spec.get("default"))
    else:
        val = spec.get("default")
    return apply_tail_ops(val, rule)
register_operation("lookup", _op_lookup)


def _op_map(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    spec = rule["map"]
    over = eval_rule(spec.get("over"))
    arr = _ensure_list(over)
    apply = spec.get("apply")
    out: List[Any] = []
    for el in arr:
        val = eval_rule(apply, ctx_override=type(ctx)(ctx.root, el, ctx.defs))
        out.append(val)
    return _emit_list_like(out, spec, rule, apply_tail_ops)
register_operation("map", _op_map)

def _op_filter(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    spec = rule["filter"]
    over = eval_rule(spec.get("over"))
    arr = _ensure_list(over)
    where = spec.get("where")
    out: List[Any] = []
    for el in arr:
        ok = Predicate.evaluate(where, eval_rule=lambda r, ctx_override=None: eval_rule(r, ctx_override=type(ctx)(ctx.root, el, ctx.defs)))
        if ok:
            out.append(el)
    return _emit_list_like(out, spec, rule, apply_tail_ops)
register_operation("filter", _op_filter)

def _op_flat_map(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    spec = rule["flat_map"]
    over = eval_rule(spec.get("over"))
    arr = _ensure_list(over)
    apply = spec.get("apply")
    out: List[Any] = []
    for el in arr:
        val = eval_rule(apply, ctx_override=type(ctx)(ctx.root, el, ctx.defs))
        if isinstance(val, list):
            out.extend(val)
        elif val is not None:
            out.append(val)
    return _emit_list_like(out, spec, rule, apply_tail_ops)
register_operation("flat_map", _op_flat_map)

def _op_unique(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    spec = rule["unique"]
    over = eval_rule(spec.get("over"))
    arr = _ensure_list(over)
    key_rule = spec.get("key")
    seen = set()
    out: List[Any] = []
    for el in arr:
        k = eval_rule(key_rule, ctx_override=type(ctx)(ctx.root, el, ctx.defs)) if key_rule else json.dumps(el, sort_keys=True, ensure_ascii=False)
        if k not in seen:
            seen.add(k)
            out.append(el)
    return _emit_list_like(out, spec, rule, apply_tail_ops)
register_operation("unique", _op_unique)

def _op_sort(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    spec = rule["sort"]
    over = eval_rule(spec.get("over"))
    arr = _ensure_list(over)
    key_rule = spec.get("key")
    reverse = bool(spec.get("reverse", False))
    if key_rule:
        decorated = []
        for el in arr:
            k = eval_rule(key_rule, ctx_override=type(ctx)(ctx.root, el, ctx.defs))
            decorated.append((k, el))
        decorated.sort(key=lambda t: (t[0] is None, t[0]), reverse=reverse)
        out = [el for _, el in decorated]
    else:
        try:
            out = sorted(arr, reverse=reverse)
        except Exception:
            out = arr
    return _emit_list_like(out, spec, rule, apply_tail_ops)
register_operation("sort", _op_sort)


def _op_group_reduce(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    spec = rule["group_reduce"]
    over = eval_rule(spec.get("over"))
    arr = _ensure_list(over)
    by_rule = spec.get("by")
    val_rule = spec.get("value")
    agg = spec.get("agg", "sum")
    as_list = bool(spec.get("as_list", False))

    groups: Dict[Any, List[float]] = {}
    for el in arr:
        subctx = type(ctx)(ctx.root, el, ctx.defs)
        key = eval_rule(by_rule, ctx_override=subctx)
        val = eval_rule(val_rule, ctx_override=subctx)
        fv = to_float(val)
        if key is None or fv is None:
            continue
        groups.setdefault(key, []).append(fv)

    out_map: Dict[Any, Any] = {}
    for k, vs in groups.items():
        if not vs:
            out_map[k] = None
        elif agg == "sum":
            out_map[k] = float(sum(vs))
        elif agg == "mean":
            out_map[k] = float(sum(vs) / len(vs))
        elif agg == "min":
            out_map[k] = float(min(vs))
        elif agg == "max":
            out_map[k] = float(max(vs))
        elif agg == "count":
            out_map[k] = int(len(vs))
        else:
            out_map[k] = None

    if as_list:
        out = [{"key": k, "value": v} for k, v in out_map.items()]
    else:
        out = out_map

    return apply_tail_ops(out, rule)
register_operation("group_reduce", _op_group_reduce)


def _op_serialize(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    spec = rule["serialize"]
    obj = eval_rule(spec.get("of"))
    try:
        val = json.dumps(obj, ensure_ascii=False)
    except Exception:
        val = None
    return apply_tail_ops(val, rule)
register_operation("serialize", _op_serialize)


def _op_merge(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    spec = rule["merge"]
    objs_rule = spec.get("objects") or []
    strategy = spec.get("strategy", "override")
    objs = [eval_rule(r) for r in objs_rule]
    objs = [o for o in objs if isinstance(o, dict)]
    if not objs:
        return apply_tail_ops(None, rule)
    if strategy == "override":
        out: Dict[str, Any] = {}
        for o in objs:
            out.update(o)
    elif strategy == "first_non_null":
        keys = {k for o in objs for k in o.keys()}
        out = {}
        for k in keys:
            val = None
            for o in objs:
                if o.get(k) is not None:
                    val = o[k]
                    break
            out[k] = val
    else:
        out = None
    return apply_tail_ops(out, rule)
register_operation("merge", _op_merge)

from ..udf import get_udf

def _op_udf(rule: Dict[str, Any], ctx, eval_rule, apply_tail_ops):
    spec = rule["udf"]
    name = spec.get("name")
    args_spec = spec.get("args", [])
    fn = get_udf(str(name)) if name else None
    if not fn:
        return apply_tail_ops(None, rule)
    args = [eval_rule(a) for a in args_spec]
    try:
        val = fn(*args)
    except Exception:
        val = None
    return apply_tail_ops(val, rule)
register_operation("udf", _op_udf)
