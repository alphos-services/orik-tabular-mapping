"""
Nested Inventory: Products with Batches
Use case: Turn product records with nested 'batches' into rows for each batch while referencing root-level fields.
- Explodes 'product.batches' into rows
- Uses rel_path for batch-level fields and path for product-level fields
- Concatenates a composite key
"""

import json

from src.engine.converter import DeclarativeConverter
from src.engine.validation import validate_mapping

mapping_json = r'''
{
  "explode": {
    "path": "product.batches",
    "emit_root_when_empty": true
  },
  "columns": {
    "product_id": { "path": "product.id" },
    "sku": { "path": "product.sku" },
    "batch_id": { "rel_path": "id" },
    "mfg_date": {
      "date_format": {
        "parse": { "rel_path": "mfg" },
        "fmt": "%Y-%m-%d"
      }
    },
    "qty": { "rel_path": "qty", "cast": "int", "default": 0 },
    "location": { "rel_path": "location", "default": "UNKNOWN" },
    "composite": { "concat": [ {"path":"product.sku"}, {"const":"#"}, {"rel_path":"id"} ] }
  }
}
'''

data_json = r'''
[
  {
    "product": {
      "id": "P-1",
      "sku": "SKU-RED-01",
      "batches": [
        {"id":"b-1","mfg":"2025-10-01T00:00:00Z","qty": 120,"location":"A1"},
        {"id":"b-2","mfg":"2025-10-05T00:00:00Z","qty": 80,"location":"A2"}
      ]
    }
  },
  {
    "product": {
      "id": "P-2",
      "sku": "SKU-BLUE-02",
      "batches": []
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
    df.to_csv("inventory_batches.csv", index=False)
