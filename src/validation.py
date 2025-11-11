from __future__ import annotations

from typing import Any, Dict, List, Tuple, Set

JsonRule = Any


class MappingError(ValueError):
    pass


SUPPORTED_CASTS: Set[str] = {"str", "int", "float", "bool"}
SUPPORTED_REDUCE: Set[str] = {"sum", "mean", "min", "max"}
SUPPORTED_PRED_OPS: Set[str] = {"exists", "eq", "gt", "lt", "regex"}
SUPPORTED_MATH_OPS: Set[str] = {"add", "sub", "mul", "div"}


def validate_mapping(mapping: Dict[str, Any], *, raise_on_error: bool = False) -> Tuple[bool, List[str]]:
    errors: List[str] = []

    def err(msg: str, path: str = "$") -> None:
        errors.append(f"{path}: {msg}")

    if not isinstance(mapping, dict):
        err("Mapping muss ein Objekt (dict) sein.")
        return _finish(errors, raise_on_error)

    if "explode" in mapping:
        exp = mapping["explode"]
        if not isinstance(exp, dict):
            err("'explode' muss ein Objekt sein.", "$.explode")
        else:
            if "path" not in exp or not isinstance(exp.get("path"), str) or not exp.get("path"):
                err("'explode.path' muss ein nicht-leerer String sein.", "$.explode.path")
            if "emit_root_when_empty" in exp and not isinstance(exp["emit_root_when_empty"], bool):
                err("'explode.emit_root_when_empty' muss bool sein.", "$.explode.emit_root_when_empty")

    cols = mapping.get("columns")
    if not isinstance(cols, dict) or not cols:
        err("'columns' muss ein nicht-leeres Objekt sein.", "$.columns")
    else:
        for col_name, rule in cols.items():
            if not isinstance(col_name, str) or not col_name.strip():
                err("Spaltenname muss ein nicht-leerer String sein.", "$.columns[<name>]")
            _validate_rule(rule, path=f"$.columns.{col_name}", add_err=err)

    return _finish(errors, raise_on_error)


def _finish(errors: List[str], raise_on_error: bool) -> Tuple[bool, List[str]]:
    if errors and raise_on_error:
        raise MappingError("Ungültiges Mapping:\n- " + "\n- ".join(errors))
    return (len(errors) == 0, errors)


def _validate_rule(rule: JsonRule, *, path: str, add_err) -> None:
    if not isinstance(rule, dict):
        return

    known_keys: Set[str] = {
        "path", "rel_path", "const", "coalesce", "math", "concat", "join",
        "index", "len", "reduce", "serialize", "date_format", "if",
        "cast", "default"
    }

    if "cast" in rule and rule["cast"] not in SUPPORTED_CASTS:
        add_err(f"Ungültiger cast: {rule['cast']} (erlaubt: {sorted(SUPPORTED_CASTS)})", path)

    if "path" in rule and not isinstance(rule["path"], str):
        add_err("'path' muss ein String sein.", f"{path}.path")
    if "rel_path" in rule and not isinstance(rule["rel_path"], str):
        add_err("'rel_path' muss ein String sein.", f"{path}.rel_path")

    if "const" in rule:
        pass

    if "coalesce" in rule:
        co = rule["coalesce"]
        if not (isinstance(co, list) and len(co) >= 1):
            add_err("'coalesce' muss eine nicht-leere Liste von Regeln sein.", f"{path}.coalesce")
        else:
            for i, sub in enumerate(co):
                _validate_rule(sub, path=f"{path}.coalesce[{i}]", add_err=add_err)

    if "math" in rule:
        m = rule["math"]
        if not (isinstance(m, list) and len(m) >= 2):
            add_err("'math' erwartet Liste [op, arg1, ...].", f"{path}.math")
        else:
            op = m[0]
            if op not in SUPPORTED_MATH_OPS:
                add_err(f"Unbekannter math-Operator '{op}'. Erlaubt: {sorted(SUPPORTED_MATH_OPS)}", f"{path}.math[0]")
            for i, sub in enumerate(m[1:], start=1):
                _validate_rule(sub, path=f"{path}.math[{i}]", add_err=add_err)

    if "concat" in rule:
        c = rule["concat"]
        if not (isinstance(c, list) and len(c) >= 1):
            add_err("'concat' muss eine nicht-leere Liste sein.", f"{path}.concat")
        else:
            for i, sub in enumerate(c):
                _validate_rule(sub, path=f"{path}.concat[{i}]", add_err=add_err)

    if "join" in rule:
        j = rule["join"]
        if not isinstance(j, dict):
            add_err("'join' muss ein Objekt sein.", f"{path}.join")
        else:
            if "over" not in j:
                add_err("'join.over' ist erforderlich.", f"{path}.join.over")
            else:
                _validate_rule(j["over"], path=f"{path}.join.over", add_err=add_err)
            if "sep" in j and not isinstance(j["sep"], str):
                add_err("'join.sep' muss ein String sein.", f"{path}.join.sep")

    if "index" in rule:
        idx = rule["index"]
        if not isinstance(idx, dict):
            add_err("'index' muss ein Objekt sein.", f"{path}.index")
        else:
            if "of" not in idx:
                add_err("'index.of' ist erforderlich.", f"{path}.index.of")
            else:
                _validate_rule(idx["of"], path=f"{path}.index.of", add_err=add_err)
            if "at" in idx and not isinstance(idx["at"], int):
                add_err("'index.at' muss ein Integer sein.", f"{path}.index.at")

    if "len" in rule:
        _validate_rule(rule["len"], path=f"{path}.len", add_err=add_err)

    if "reduce" in rule:
        r = rule["reduce"]
        if not isinstance(r, dict):
            add_err("'reduce' muss ein Objekt sein.", f"{path}.reduce")
        else:
            if "over" not in r:
                add_err("'reduce.over' ist erforderlich.", f"{path}.reduce.over")
            else:
                _validate_rule(r["over"], path=f"{path}.reduce.over", add_err=add_err)
            if "op" not in r or r["op"] not in SUPPORTED_REDUCE:
                add_err(f"'reduce.op' muss in {sorted(SUPPORTED_REDUCE)} liegen.", f"{path}.reduce.op")

    if "serialize" in rule:
        s = rule["serialize"]
        if not isinstance(s, dict) or "of" not in s:
            add_err("'serialize' erwartet Objekt mit Feld 'of'.", f"{path}.serialize")
        else:
            _validate_rule(s["of"], path=f"{path}.serialize.of", add_err=add_err)

    if "date_format" in rule:
        d = rule["date_format"]
        if not isinstance(d, dict):
            add_err("'date_format' muss ein Objekt sein.", f"{path}.date_format")
        else:
            if "parse" not in d:
                add_err("'date_format.parse' ist erforderlich.", f"{path}.date_format.parse")
            else:
                _validate_rule(d["parse"], path=f"{path}.date_format.parse", add_err=add_err)
            if "fmt" in d and not isinstance(d["fmt"], str):
                add_err("'date_format.fmt' muss ein String sein.", f"{path}.date_format.fmt")
            if "fmt_in" in d and not isinstance(d["fmt_in"], str):
                add_err("'date_format.fmt_in' muss ein String sein.", f"{path}.date_format.fmt_in")

    if "if" in rule:
        i = rule["if"]
        if not isinstance(i, dict):
            add_err("'if' muss ein Objekt sein.", f"{path}.if")
        else:
            if "cond" not in i:
                add_err("'if.cond' ist erforderlich.", f"{path}.if.cond")
            else:
                _validate_predicate(i["cond"], path=f"{path}.if.cond", add_err=add_err)
            if "then" not in i:
                add_err("'if.then' ist erforderlich.", f"{path}.if.then")
            else:
                _validate_rule(i["then"], path=f"{path}.if.then", add_err=add_err)
            if "else" in i:
                _validate_rule(i["else"], path=f"{path}.if.else", add_err=add_err)

    unknown = set(rule.keys()) - known_keys
    if unknown:
        add_err(f"Unbekannte Schlüssel in Regel: {sorted(unknown)}", path)


def _validate_predicate(pred: Dict[str, Any], *, path: str, add_err) -> None:
    if not isinstance(pred, dict):
        add_err("Predicate muss ein Objekt sein.", path)
        return

    op = pred.get("op")
    if op not in SUPPORTED_PRED_OPS:
        add_err(f"Ungültiges Predicate 'op'. Erlaubt: {sorted(SUPPORTED_PRED_OPS)}", f"{path}.op")
        return

    if op == "exists":
        if "arg" not in pred:
            add_err("'exists' benötigt Feld 'arg'.", f"{path}.arg")
        else:
            _validate_rule(pred["arg"], path=f"{path}.arg", add_err=add_err)
    else:
        if "a" not in pred:
            add_err("Predicate benötigt Feld 'a'.", f"{path}.a")
        else:
            _validate_rule(pred["a"], path=f"{path}.a", add_err=add_err)
        if "b" not in pred:
            add_err("Predicate benötigt Feld 'b'.", f"{path}.b")
        else:
            _validate_rule(pred["b"], path=f"{path}.b", add_err=add_err)


class DeclarativeConverterWithValidation:
    def __init__(self, mapping: Dict[str, Any]) -> None:
        ok, errs = validate_mapping(mapping)
        if not ok:
            raise MappingError("Ungültiges Mapping:\n- " + "\n- ".join(errs))
        self.mapping = mapping

    @staticmethod
    def validate_mapping(mapping: Dict[str, Any], *, raise_on_error: bool = False) -> Tuple[bool, List[str]]:
        return validate_mapping(mapping, raise_on_error=raise_on_error)
