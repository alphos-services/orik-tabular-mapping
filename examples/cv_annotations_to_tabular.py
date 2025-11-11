"""
Computer Vision Annotations to Tabular
Use case: Convert image annotations (bounding boxes) into a training-ready table.
- Explodes 'annotations' -> one row per object
- Index access for first label example
- Joins multiple labels per object
"""

import json

from src.converter import DeclarativeConverter
from src.validation import validate_mapping

mapping_json = r'''
{
  "explode": {
    "path": "annotations",
    "emit_root_when_empty": false
  },
  "columns": {
    "image_id": { "path": "image.id" },
    "image_url": { "path": "image.url" },

    "x": { "rel_path": "bbox.x", "cast": "float" },
    "y": { "rel_path": "bbox.y", "cast": "float" },
    "w": { "rel_path": "bbox.w", "cast": "float" },
    "h": { "rel_path": "bbox.h", "cast": "float" },

    "labels_joined": { "join": { "over": {"rel_path": "labels"}, "sep": "|" } },
    "first_label": { "index": { "of": {"rel_path": "labels"}, "at": 0 } },

    "confidence": { "rel_path": "confidence", "cast": "float", "default": 1.0 }
  }
}
'''

data_json = r'''
[
  {
    "image": {"id":"img-001","url":"https://cdn.example/1.jpg"},
    "annotations": [
      {"bbox":{"x":10,"y":20,"w":100,"h":80},"labels":["person"],"confidence":0.98},
      {"bbox":{"x":200,"y":35,"w":60,"h":60},"labels":["dog","pet"],"confidence":0.91}
    ]
  },
  {
    "image": {"id":"img-002","url":"https://cdn.example/2.jpg"},
    "annotations": []
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
    df.to_csv("cv_annotations.csv", index=False)
