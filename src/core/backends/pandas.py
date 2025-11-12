from typing import List, Dict, Any

import pandas as pd


class DataFrameBackend:
    def to_dataframe(self, rows: List[Dict[str, Any]]) -> Any:
        raise NotImplementedError

    def concat(self, frames: List[Any]) -> Any:
        raise NotImplementedError


class PandasBackend(DataFrameBackend):
    def to_dataframe(self, rows: List[Dict[str, Any]]) -> pd.DataFrame:
        return pd.DataFrame(rows)

    def concat(self, frames: List[pd.DataFrame]) -> pd.DataFrame:
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)