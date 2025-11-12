from typing import Dict, Any

from src.core import DeclarativeConverter, MappingError, get_registry, EngineConfig

TAIL_KEYS = {"default", "cast", "on_error"}


def is_mapping_valid(mapping: dict) -> bool:
    try:
        DeclarativeConverter(mapping)
        return True
    except MappingError as err:
        print("Invalid mapping:", err)
        return False


def validate_with_warnings(mapping: Dict[str, Any]) -> Dict[str, Any]:
    out = {"ok": False, "errors": [], "warnings": []}

    try:
        DeclarativeConverter(mapping)
    except MappingError as e:
        out["errors"].append(str(e))
        return out

    reg = get_registry()

    def walk(node: Any, path: str = "$") -> None:
        if isinstance(node, dict):
            op_keys = [k for k in node.keys() if k in reg.head_keys]
            tail_only = all(k in TAIL_KEYS for k in node.keys())
            if not op_keys and not tail_only:
                if any(k not in TAIL_KEYS for k in node.keys()):
                    out["warnings"].append(f"Unknown/empty op at {path}: keys={list(node.keys())}")

            if len(op_keys) > 1:
                out["warnings"].append(f"Multiple ops at {path}: {op_keys} (registry precedence applies)")

            for k, v in node.items():
                walk(v, f"{path}.{k}")

        elif isinstance(node, list):
            for i, v in enumerate(node):
                walk(v, f"{path}[{i}]")

    walk(mapping.get("columns", {}), "$.columns")
    out["ok"] = len(out["errors"]) == 0
    return out


def dry_run(mapping: dict, sample):
    try:
        conv = DeclarativeConverter(mapping, config=EngineConfig(trace_enabled=True))
    except MappingError as e:
        return {"ok": False, "stage": "structure", "error": str(e)}

    try:
        if isinstance(sample, list):
            df = conv.to_dataframe_batch(sample)
            trace = conv.trace(sample[0]) if sample else {"rows_emitted": 0, "columns_trace": {}}
        else:
            df = conv.to_dataframe_single(sample)
            trace = conv.trace(sample)

        return {"ok": True, "rows": len(df), "columns": list(df.columns), "trace": trace}
    except Exception as e:

        return {"ok": False, "stage": "evaluation", "error": repr(e)}