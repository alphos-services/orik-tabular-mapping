"""
E-commerce Order Normalization
Use case: Turn heterogeneous order payloads into a line-item table.
- Explodes 'order.items' into rows
- Computes line amounts via math (qty * unit_price)
- Joins array tags
- Coalesces missing SKU
- Formats order placed time
"""

import json

from src.converter import DeclarativeConverter
from src.validation import validate_mapping

mapping_json = r'''
{
  "explode": {
    "path": "order.items",
    "emit_root_when_empty": true
  },
  "columns": {
    "order_id": { "path": "order.id" },
    "placed_at": {
      "date_format": {
        "parse": { "path": "order.placed_at" },
        "fmt": "%Y-%m-%d %H:%M:%S"
      }
    },
    "customer_id": { "path": "customer.id" },
    "country": { "path": "shipping.country", "default": "UNKNOWN" },

    "sku": { "coalesce": [ {"rel_path": "sku"}, {"const": "MISSING-SKU"} ] },
    "qty": { "rel_path": "qty", "cast": "int", "default": 1 },
    "unit_price": { "rel_path": "unit_price", "cast": "float", "default": 0.0 },
    "line_total": {
      "math": ["mul",
        {"rel_path": "qty"},
        {"rel_path": "unit_price"}
      ]
    },

    "tags": { "join": { "over": {"path": "order.tags"}, "sep": "|" } },

    "high_value_flag": {
      "if": {
        "cond": { "op": "gt", "a": {"path":"order.total"}, "b": {"const": 500} },
        "then": {"const": 1},
        "else": {"const": 0}
      }
    }
  }
}
'''

data_json = r'''
[
  {
    "order": {
      "id": "O-1001",
      "placed_at": "2025-10-30T09:15:00Z",
      "total": 635.50,
      "tags": ["VIP", "BlackFriday"],
      "items": [
        {"sku": "SKU-AAA", "qty": 2, "unit_price": 120.0},
        {"qty": 1, "unit_price": 395.5}
      ]
    },
    "customer": { "id": "C-9" },
    "shipping": { "country": "DE" }
  },
  {
    "order": {
      "id": "O-1002",
      "placed_at": "2025-10-30T10:00:00Z",
      "total": 49.99,
      "tags": [],
      "items": []
    },
    "customer": { "id": "C-10" },
    "shipping": { "country": "NL" }
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
    df.to_csv("ecommerce_line_items.csv", index=False)
