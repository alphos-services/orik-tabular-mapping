"""
Health Wearables
Use case: Normalize patient wearable data with readings per minute.
- Explodes 'readings' into rows
- Conditional flagging (tachycardia threshold)
- Coalesce missing metrics
- Date formatting for display-ready timestamps
"""

import json

from src.converter import DeclarativeConverter
from src.validation import validate_mapping

mapping_json = r'''
{
  "explode": {
    "path": "readings",
    "emit_root_when_empty": true
  },
  "columns": {
    "patient_id": { "path": "patient.id" },
    "ts": {
      "date_format": {
        "parse": { "rel_path": "ts" },
        "fmt": "%Y-%m-%d %H:%M:%S"
      }
    },

    "hr_bpm": { "coalesce": [ {"rel_path":"hr"}, {"const": 0} ], "cast": "int" },
    "spo2": { "coalesce": [ {"rel_path":"spo2"}, {"const": 0} ], "cast": "int" },
    "steps": { "rel_path": "steps", "cast": "int", "default": 0 },

    "tachy_flag": {
      "if": {
        "cond": { "op": "gt", "a": {"rel_path":"hr"}, "b": {"const": 100} },
        "then": {"const": 1},
        "else": {"const": 0}
      }
    },

    "day_bucket": {
      "date_format": {
        "parse": { "rel_path": "ts" },
        "fmt": "%Y-%m-%d"
      }
    }
  }
}
'''

data_json = r'''
[
  {
    "patient": { "id": "P-123" },
    "readings": [
      {"ts": "2025-11-11T06:00:00Z", "hr": 72, "spo2": 98, "steps": 40},
      {"ts": "2025-11-11T06:01:00Z", "hr": 104, "spo2": 97, "steps": 42}
    ]
  },
  {
    "patient": { "id": "P-456" },
    "readings": []
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
    df.to_csv("wearables_readings.csv", index=False)
