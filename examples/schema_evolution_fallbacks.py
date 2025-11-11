"""
Schema Evolution & Fallback Paths
Use case: Evolving producers with inconsistent field names; support both legacy and new paths.
- Coalesce between multiple potential JSON paths (v1 vs v2)
- Default and cast ensure stable CSV schema
- Demonstrates robust ingestion across schema versions
"""

import json

from src.converter import DeclarativeConverter
from src.validation import validate_mapping

mapping_json = r'''
{
  "columns": {
    "event_id": { "coalesce": [ {"path":"id"}, {"path":"event.id"} ] },
    "device_id": { "coalesce": [ {"path":"meta.device_id"}, {"path":"device.id"} ] },
    "ts": {
      "date_format": {
        "parse": { "coalesce": [ {"path":"time"}, {"path":"timestamp"} ] },
        "fmt": "%Y-%m-%d %H:%M:%S"
      }
    },
    "temp_c": { "coalesce": [ {"path":"sensor.temp_c"}, {"path":"sensor.temperature"} ], "cast":"float", "default": 0.0 },
    "humidity_pct": { "path":"sensor.humidity", "cast":"float", "default": 0.0 },
    "schema_version": { "coalesce": [ {"path":"_schema"}, {"const":"unknown"} ] }
  }
}
'''

data_json = r'''
[
  {
    "id": "e-1",
    "time": "2025-10-01T10:00:00Z",
    "meta": {"device_id": "dev-legacy"},
    "sensor": {"temperature": 21.7, "humidity": 44.0},
    "_schema": "v1"
  },
  {
    "event": {"id": "e-2"},
    "timestamp": "2025-10-01T10:05:00Z",
    "device": {"id": "dev-new"},
    "sensor": {"temp_c": 22.1, "humidity": 45.2},
    "_schema": "v2"
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
    df.to_csv("schema_evolution.csv", index=False)
