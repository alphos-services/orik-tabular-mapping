from __future__ import annotations
import re
from typing import Any, Optional, List, Tuple, Union


class PathSyntaxError(ValueError):
    pass


class PathResolver:
    """
    Resolve dotted paths into nested dict/list structures.

    Supported selectors per segment:
      - key                  e.g. user
      - [N]                  index
      - [*]                  wildcard (collect)
      - [?field==value]      filter (==,!=,>,<,>=,<=,~= regex)
      - ?[N]                 safe index (returns None if OOB)

    Examples:
      items[*].price
      user.emails[?type=="work"]?[0].value
      data.list[?qty>0][*].sku
    """

    _token_head = re.compile(r"([^. \[\]]+)(.*)$")  # name then rest
    _bracket = re.compile(r"^\[(.*?)\](.*)$")

    @classmethod
    def get(cls, obj: Any, path: Optional[str]) -> Any:
        if path is None:
            return None
        cur = obj
        segments = path.split(".")
        for seg in segments:
            if cur is None:
                return None

            if isinstance(cur, list):
                mapped: List[Any] = []
                for el in cur:
                    if isinstance(el, dict):
                        res = cls._apply_segment(el, seg)
                    else:
                        res = None
                    if isinstance(res, list):
                        mapped.extend(res)
                    else:
                        mapped.append(res)
                cur = mapped
                continue

            cur = cls._apply_segment(cur, seg)

        return cur

    @classmethod
    def _apply_segment(cls, base: Any, segment: str) -> Any:
        m = cls._token_head.match(segment)
        if not m:
            return None
        key, rest = m.group(1), m.group(2)

        if isinstance(base, dict):
            cur = base.get(key)
        else:
            return None

        while rest:
            if rest.startswith("?["):
                sub = rest[1:]
                bm = cls._bracket.match(sub)
                if not bm:
                    raise PathSyntaxError(f"Malformed path at '{rest}'")
                selector, rest = bm.group(1), bm.group(2)
                cur = cls._apply_selector(cur, selector, safe=True)
                continue

            bm = cls._bracket.match(rest)
            if not bm:
                raise PathSyntaxError(f"Malformed path at '{rest}'")
            selector, rest = bm.group(1), bm.group(2)
            cur = cls._apply_selector(cur, selector, safe=False)

        return cur

    @classmethod
    def _apply_selector(cls, cur: Any, selector: str, *, safe: bool) -> Any:
        if selector == "*":
            if isinstance(cur, list):
                return cur
            if isinstance(cur, dict):
                return list(cur.values())
            return None

        # index
        if selector.isdigit():
            idx = int(selector)
            if not isinstance(cur, list):
                return None
            if 0 <= idx < len(cur):
                return cur[idx]
            return None if safe else None

        if selector.startswith("?"):
            if not isinstance(cur, list):
                return None
            predicate = selector[1:]
            field, op, lit = cls._parse_predicate(predicate)
            out = []
            for el in cur:
                if not isinstance(el, dict):
                    continue
                val = el.get(field)
                if cls._match(val, op, lit):
                    out.append(el)
            return out

        return None

    @staticmethod
    def _parse_predicate(expr: str) -> Tuple[str, str, Union[str, float]]:
        for op in ("==", "!=", ">=", "<=", ">", "<", "~="):
            if op in expr:
                left, right = expr.split(op, 1)
                left = left.strip()
                right = right.strip().strip('"').strip("'")
                try:
                    num = float(right)
                    return left, op, num
                except Exception:
                    return left, op, right
        return expr.strip(), "==", ""

    @staticmethod
    def _match(val: Any, op: str, lit: Union[str, float]) -> bool:
        import re as _re
        if op == "==":
            return val == lit
        if op == "!=":
            return val != lit
        try:
            vf = float(val) if val is not None else None
        except Exception:
            vf = None
        if op == ">" and vf is not None and isinstance(lit, (int, float)):
            return vf > lit
        if op == "<" and vf is not None and isinstance(lit, (int, float)):
            return vf < lit
        if op == ">=" and vf is not None and isinstance(lit, (int, float)):
            return vf >= lit
        if op == "<=" and vf is not None and isinstance(lit, (int, float)):
            return vf <= lit
        if op == "~=":
            if val is None:
                return False
            try:
                return _re.search(str(lit), str(val)) is not None
            except Exception:
                return False
        return False
