"""
Event Stream Normalization
Use case: Mixed event types in a single stream; normalize to a flat table with type-specific projections.
- Conditional columns based on 'type'
- Coalesce for optional fields
- Regex predicate to classify messages
"""

import json

from src.engine.converter import DeclarativeConverter
from src.engine.validation import validate_mapping

mapping_json = r'''
{
  "columns": {
    "event_id": { "path": "id" },
    "type": { "path": "type" },
    "ts": {
      "date_format": {
        "parse": { "path": "ts" },
        "fmt": "%Y-%m-%d %H:%M:%S"
      }
    },

    "msg": { "coalesce": [ {"path":"msg"}, {"path":"message"} ] },
    "user": { "coalesce": [ {"path":"user.id"}, {"path":"actor"} ] },

    "is_error": {
      "if": {
        "cond": { "op": "regex", "a": {"coalesce":[{"path":"msg"}, {"path":"message"}]}, "b": {"const":"(?i)error|fail|exception"} },
        "then": {"const": 1},
        "else": {"const": 0}
      }
    },

    "payload_size": { "len": { "coalesce": [ {"path":"payload"}, {"const": ""} ] } },

    "value_for_metric": {
      "if": {
        "cond": { "op": "eq", "a": {"path":"type"}, "b": {"const":"metric"} },
        "then": {"path":"payload.value"},
        "else": {"const": null}
      }
    }
  }
}
'''

data_json = r'''
[
  {"id":"e-10","type":"log","ts":"2025-10-20T08:00:01Z","msg":"User login ok","user":{"id":"u-1"}},
  {"id":"e-11","type":"metric","ts":"2025-10-20T08:00:02Z","payload":{"name":"cpu","value":0.73},"actor":"u-2"},
  {"id":"e-12","type":"log","ts":"2025-10-20T08:00:03Z","message":"ERROR: disk full","user":{"id":"root"}}
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
    df.to_csv("event_stream.csv", index=False)
