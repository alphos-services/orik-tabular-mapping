from __future__ import annotations

from datetime import datetime
from typing import Any, Optional, List
import pandas as pd

try:
    import torch
except Exception:
    torch = None


def to_float(x: Any) -> Optional[float]:
    if x is None:
        return None

    try:
        return float(x)

    except (TypeError, ValueError):
        return None


def mul_all(nums: List[float]) -> float:
    out = 1.0
    for n in nums:
        out *= n

    return out


def format_date(src: Any, out_fmt: str, in_fmt: Optional[str] = None) -> Optional[str]:
    if src is None:
        return None

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


def df_to_tensor(df: pd.DataFrame):
    if torch is None:
        raise RuntimeError("PyTorch is not installed. Please 'pip install torch'.")
    
    numeric = df.select_dtypes(include=["number", "bool"]).astype(float)
    return torch.tensor(numeric.values, dtype=torch.float32)
