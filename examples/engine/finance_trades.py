"""
Finance Trades & PnL
Use case: Normalize raw trades and derive P&L columns.
- No explode: one row per trade event
- Math for notional, fee, net cash flow
- Predicate-based high_notional flag
- Coalesce missing counterparty
"""

import json

from src.engine.converter import DeclarativeConverter
from src.engine.validation import validate_mapping

mapping_json = r'''
{
  "columns": {
    "trade_id": { "path": "trade.id" },
    "timestamp": {
      "date_format": {
        "parse": { "path": "trade.ts" },
        "fmt": "%Y-%m-%d %H:%M:%S"
      }
    },
    "symbol": { "path": "trade.symbol" },
    "side": { "path": "trade.side" },

    "qty": { "path": "trade.qty", "cast": "float" },
    "price": { "path": "trade.price", "cast": "float" },
    "notional": { "math": ["mul", {"path":"trade.qty"}, {"path":"trade.price"}] },

    "fee_rate": { "path": "trade.fee_rate", "cast": "float", "default": 0.001 },
    "fee": { "math": ["mul", {"path":"trade.qty"}, {"path":"trade.price"}, {"path":"trade.fee_rate"}] },

    "gross_cash": {
      "math": [
        "mul",
        {"path":"trade.qty"},
        {"path":"trade.price"},
        { "if": {
            "cond": { "op":"eq", "a":{"path":"trade.side"}, "b":{"const":"BUY"} },
            "then": {"const": -1},
            "else": {"const": 1}
        }}
      ]
    },

    "net_cash": { "math": ["add", {"path":"gross_cash"}, {"math":["mul", {"const": -1}, {"path":"fee"}]}] },

    "counterparty": { "coalesce": [ {"path":"trade.cp"}, {"const":"UNKNOWN"} ] },

    "high_notional": {
      "if": {
        "cond": { "op": "gt", "a": {"path": "notional"}, "b": {"const": 100000} },
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
    "trade": {
      "id": "T-9001",
      "ts": "2025-11-11T09:15:00Z",
      "symbol": "AAPL",
      "side": "BUY",
      "qty": 500,
      "price": 210.50,
      "fee_rate": 0.0008
    }
  },
  {
    "trade": {
      "id": "T-9002",
      "ts": "2025-11-11T09:16:30Z",
      "symbol": "TSLA",
      "side": "SELL",
      "qty": 50,
      "price": 240.20
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

    # NOTE:
    # 'gross_cash' and 'fee' are derived columns in mapping. For referencing them
    # within the same mapping (like 'net_cash'), many users pre-compute in post-processing.
    # In this declarative engine, each column is evaluated independently; to reuse,
    # repeat the expression (as done for 'net_cash'). Another approach is to post-process.

    conv = DeclarativeConverter(mapping)
    df = conv.to_dataframe_batch(payload)
    print(df)
    df.to_csv("trades_pnl.csv", index=False)
