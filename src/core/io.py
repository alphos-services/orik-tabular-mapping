from __future__ import annotations
from typing import Iterable, Dict, Any, List, Optional
import json
import pandas as pd

from .engine import DeclarativeConverter


def stream_jsonl_to_csv(converter: DeclarativeConverter, in_path: str, out_path: str, *, batch_size: int = 10_000, include_header: bool = True) -> None:
    batch: List[Dict[str, Any]] = []
    header_written = False
    with open(in_path, "r", encoding="utf-8") as fin, open(out_path, "w", encoding="utf-8", newline="") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue

            batch.append(json.loads(line))
            if len(batch) >= batch_size:
                df = converter.to_dataframe_batch(batch)
                df.to_csv(fout, header=(include_header and not header_written), index=False, mode="a")
                header_written = True
                batch.clear()

        if batch:
            df = converter.to_dataframe_batch(batch)
            df.to_csv(fout, header=(include_header and not header_written), index=False, mode="a")


def stream_jsonl_to_parquet(converter: DeclarativeConverter, in_path: str, out_path: str, *, batch_size: int = 10_000, compression: Optional[str] = "snappy") -> None:
    try:
        import pyarrow as pa  # noqa
        import pyarrow.parquet as pq  # noqa
    except Exception as e:
        raise RuntimeError("pyarrow is required for Parquet output. Please 'pip install pyarrow'.") from e

    writer = None
    batch_rows: List[Dict[str, Any]] = []
    for line in open(in_path, "r", encoding="utf-8"):
        line = line.strip()
        if not line:
            continue

        batch_rows.append(json.loads(line))
        if len(batch_rows) >= batch_size:
            df = converter.to_dataframe_batch(batch_rows)
            table = pa.Table.from_pandas(df)
            if writer is None:
                writer = pq.ParquetWriter(out_path, table.schema, compression=compression)

            writer.write_table(table)
            batch_rows.clear()

    if batch_rows:
        import pyarrow as pa
        import pyarrow.parquet as pq
        df = converter.to_dataframe_batch(batch_rows)
        table = pa.Table.from_pandas(df)
        if writer is None:
            writer = pq.ParquetWriter(out_path, table.schema, compression=compression)

        writer.write_table(table)

    if writer is not None:
        writer.close()
