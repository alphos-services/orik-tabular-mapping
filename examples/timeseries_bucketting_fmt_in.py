"""
Timeseries Bucketing with Custom Input Format
Use case: Producer sends non-ISO timestamps; normalize and bucket by day.
- date_format with 'fmt_in' for custom parsing
- day bucket derived for downstream grouping
"""

import json

from src.converter import DeclarativeConverter
from src.validation import validate_mapping

mapping_json = r'''
{
  "columns": {
    "site": { "path": "site" },
    "sensor": { "path": "sensor" },
    "timestamp": {
      "date_format": {
        "parse": { "path": "ts_raw" },
        "fmt": "%Y-%m-%d %H:%M:%S",
        "fmt_in": "%d/%m/%Y %H:%M:%S"
      }
    },
    "day_bucket": {
      "date_format": {
        "parse": { "path": "ts_raw" },
        "fmt": "%Y-%m-%d",
        "fmt_in": "%d/%m/%Y %H:%M:%S"
      }
    },
    "value": { "path": "value", "cast": "float" }
  }
}
'''

data_json = r'''
[
  {"site":"A","sensor":"temp","ts_raw":"11/11/2025 06:00:00","value":21.4},
  {"site":"A","sensor":"temp","ts_raw":"11/11/2025 07:00:00","value":22.1},
  {"site":"B","sensor":"vib","ts_raw":"11/11/2025 07:05:00","value":0.18}
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
    df.to_csv("timeseries_custom_fmt.csv", index=False)
