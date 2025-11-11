"""
Mobility GPS Tracks
Use case: Normalize trips with variable numbers of GPS points.
- Explodes 'trip.points' into rows (one row per point)
- Coordinates concatenated as strings
- Index-based selection and length over arrays
- Reduces average speed from per-point speeds
"""

import json

from src.converter import DeclarativeConverter
from src.validation import validate_mapping

mapping_json = r'''
{
  "explode": {
    "path": "trip.points",
    "emit_root_when_empty": true
  },
  "columns": {
    "trip_id": { "path": "trip.id" },
    "vehicle": { "path": "vehicle.id" },
    "started_at": {
      "date_format": {
        "parse": { "path": "trip.started_at" },
        "fmt": "%Y-%m-%d %H:%M:%S"
      }
    },

    "point_idx": { "rel_path": "i", "cast": "int", "default": -1 },
    "lat": { "rel_path": "lat", "cast": "float" },
    "lon": { "rel_path": "lon", "cast": "float" },
    "coord": { "concat": [ {"rel_path":"lat"}, {"const": ","}, {"rel_path":"lon"} ] },

    "first_coord": {
      "index": { "of": {"path": "trip.coords"}, "at": 0 }
    },
    "num_points": { "len": {"path":"trip.points"} },

    "avg_speed_kmh": {
      "reduce": { "over": {"path":"trip.speeds_kmh"}, "op": "mean" }
    }
  }
}
'''

data_json = r'''
[
  {
    "trip": {
      "id": "T-01",
      "started_at": "2025-11-10T07:30:00Z",
      "points": [
        {"i": 0, "lat": 52.5201, "lon": 13.4049},
        {"i": 1, "lat": 52.5202, "lon": 13.4055}
      ],
      "coords": ["52.5201,13.4049", "52.5202,13.4055"],
      "speeds_kmh": [38.2, 41.0, 35.5]
    },
    "vehicle": { "id": "car-77" }
  },
  {
    "trip": {
      "id": "T-02",
      "started_at": "2025-11-10T08:00:00Z",
      "points": [],
      "coords": [],
      "speeds_kmh": []
    },
    "vehicle": { "id": "car-88" }
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
    df.to_csv("gps_points.csv", index=False)
