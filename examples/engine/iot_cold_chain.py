"""
IoT Cold-Chain Sensors
Use case: Normalize temperature/humidity telemetry where devices upload batches of per-minute samples.
- Explodes 'sensor.samples' into rows
- Derives state column ("hot"/"ok") via an if-predicate
- Formats timestamps
- Includes list operations (reduce mean over root-level 'sensor.values')
"""

import json

from src.engine.converter import DeclarativeConverter
from src.engine.validation import validate_mapping

mapping_json = r'''
{
  "explode": {
    "path": "sensor.samples",
    "emit_root_when_empty": true
  },
  "columns": {
    "device": { "path": "meta.device_id" },
    "time": {
      "date_format": {
        "parse": {"path": "timestamp"},
        "fmt": "%Y-%m-%d %H:%M:%S"
      }
    },
    "t_c": { "path": "sensor.temp", "cast": "float" },
    "humidity": { "path": "sensor.humidity", "cast": "float" },

    "sample_index": { "rel_path": "index", "cast": "int", "default": -1 },
    "x": { "rel_path": "x", "cast": "float" },
    "y": { "rel_path": "y", "cast": "float" },

    "sum_xy": { "math": ["add", {"rel_path": "x"}, {"rel_path": "y"}] },
    "root_mean": { "reduce": { "over": {"path": "sensor.values"}, "op": "mean" } },

    "state": {
      "if": {
        "cond": { "op": "gt", "a": {"path":"sensor.temp"}, "b": {"const": 8} },
        "then": {"const": "hot"},
        "else": {"const": "ok"}
      }
    }
  }
}
'''

data_json = r'''
[
  {
    "meta": { "device_id": "truck-001" },
    "timestamp": "2025-11-11T12:00:00Z",
    "sensor": {
      "temp": 7.5,
      "humidity": 54.2,
      "values": [7.1, 7.3, 7.5, 7.8],
      "samples": [
        {"index": 0, "x": 1.0, "y": 2.0},
        {"index": 1, "x": 0.9, "y": 2.1}
      ]
    }
  },
  {
    "meta": { "device_id": "truck-002" },
    "timestamp": "2025-11-11T12:05:00Z",
    "sensor": {
      "temp": 9.2,
      "humidity": 61.0,
      "values": [8.9, 9.1, 9.2],
      "samples": []
    }
  }
]
'''

if __name__ == "__main__":
    mapping = json.loads(mapping_json)
    payload = json.loads(data_json)

    ok, errs = validate_mapping(mapping)
    if not ok:
        raise SystemExit("Invalid mapping:\n- " + "\n- ".join(errs))

    conv = DeclarativeConverter(mapping)
    df = conv.to_dataframe_batch(payload)
    print(df)

    df.to_csv("cold_chain_samples.csv", index=False)

    # Optional tensor (numeric columns only)
    try:
        tensor = conv.to_tensor_batch(payload)
        print("Tensor shape:", tensor.shape)
    except RuntimeError:
        print("Torch not installed; skipping tensor.")
