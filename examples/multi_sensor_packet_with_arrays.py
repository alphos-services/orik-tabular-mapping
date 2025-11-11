"""
Multi-Sensor Packet with Array Stats
Use case: Each record contains arrays of readings from multiple sensors; produce per-record aggregates.
- No explode -> one row per packet
- Reduce over arrays (min/max/mean/sum)
- Join arrays for audit/debugging in CSV
"""

import json

from src.converter import DeclarativeConverter
from src.validation import validate_mapping

mapping_json = r'''
{
  "columns": {
    "packet_id": { "path": "packet.id" },
    "device": { "path": "device.id" },
    "received_at": {
      "date_format": {
        "parse": { "path": "meta.received" },
        "fmt": "%Y-%m-%d %H:%M:%S"
      }
    },
    "t_mean": { "reduce": { "over": {"path":"sensors.temp_c"}, "op": "mean" } },
    "t_min":  { "reduce": { "over": {"path":"sensors.temp_c"}, "op": "min" } },
    "t_max":  { "reduce": { "over": {"path":"sensors.temp_c"}, "op": "max" } },
    "v_sum":  { "reduce": { "over": {"path":"sensors.vibration_g"}, "op": "sum" } },
    "temp_series": { "join": { "over": {"path":"sensors.temp_c"}, "sep": "|" } },
    "vib_series":  { "join": { "over": {"path":"sensors.vibration_g"}, "sep": "|" } }
  }
}
'''

data_json = r'''
[
  {
    "packet": {"id": "p-100"},
    "device": {"id": "motor-1"},
    "meta": {"received": "2025-11-01T14:10:00Z"},
    "sensors": {
      "temp_c": [52.1, 53.4, 55.0],
      "vibration_g": [0.12, 0.18, 0.10]
    }
  },
  {
    "packet": {"id": "p-101"},
    "device": {"id": "motor-2"},
    "meta": {"received": "2025-11-01T14:12:00Z"},
    "sensors": {
      "temp_c": [48.9],
      "vibration_g": []
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
    df.to_csv("multi_sensor_packets.csv", index=False)
