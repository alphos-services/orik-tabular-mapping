# ORIK – Declarative Tabular Mapping

> Turn messy, nested **JSON** into clean **tables** — **declaratively**.
> No brittle ETL code. No vendor lock-in. Just mappings.
>
> Built & maintained by **Alphos-Services GmbH**.

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](#license)
[![Python](https://img.shields.io/badge/Python-3.9%2B-blue)](#requirements)
[![Pandas](https://img.shields.io/badge/Pandas-Required-success)](#requirements)
[![PyTorch](https://img.shields.io/badge/PyTorch-Optional-orange)](#tensor-output)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](#contributing)

> If this saves you time, **please ⭐ star the repo** — it helps a ton.

---

## Why ORIK?

Modern data ingestion means **heterogeneous JSON** from many producers. Writing per-producer Python transforms is error-prone and hard to scale.

**ORIK** gives you:

* A **JSON-only mapping protocol** to describe how nested JSON becomes flat, typed tables
* A **modular, pluggable Python engine** that executes those mappings
* A **fluent Builder API** for dev-ergonomics (optional)
* Production features: **array explosion**, **list transforms**, **math & string ops**, **dates/tz**, **conditionals**, **aggregations**, **UDFs**, **schema validation**, **tracing**, **streaming IO**, and optional **tensor output**

Perfect for **IoT telemetry**, **events**, **e-commerce**, **timeseries**, **annotations**, and more.

---

## Table of Contents

* [Features at a Glance](#features-at-a-glance)
* [30-Second Quickstart](#30-second-quickstart)
* [Developer Quickstart (Builder API)](#developer-quickstart-builder-api)
* [Mapping Language (JSON)](#mapping-language-json)
* [Advanced Recipes](#advanced-recipes)

  * [Explode & Explode-Join](#explode--explode-join)
  * [List Transforms with Friendly Output](#list-transforms-with-friendly-output)
  * [Aggregations that “just work”](#aggregations-that-just-work)
  * [Dates & Timezones](#dates--timezones)
  * [Conditionals & Predicates](#conditionals--predicates)
  * [Lookups & Merging](#lookups--merging)
  * [UDFs (User-Defined Functions)](#udfs-user-defined-functions)
  * [Custom Operations (Plugins)](#custom-operations-plugins)
  * [Streaming IO (JSONL → CSV/Parquet)](#streaming-io-jsonl--csvparquet)
  * [Tensor Output](#tensor-output)
* [Architecture (Extensible by Design)](#architecture-extensible-by-design)
* [Contributing](#contributing)
* [Security](#security)
* [License](#license)
* [Contact](#contact)

---

## Features at a Glance

* **Declarative only** — mappings are pure JSON (diffable, lintable, versionable)
* **Pluggable ops registry** — add operations by registering a function; **no engine edits**
* **Ergonomic Builder API** — chainable `MappingBuilder()` & `Rule()` for dev happiness
* **Powerful paths** — dot-paths with `[*]` wildcards, filters `[?field=="x"]`, and safe index `?[0]`
* **Array handling**

  * `explode` — one row per array item
  * `explode_join` — Cartesian join of two arrays
  * **List transforms** — `map`, `filter`, `flat_map`, `unique`, `sort` with output **emit modes** (`list`, `json`, `count`, `join`)
* **Math & strings** — `add|sub|mul|div`, `concat`, `join`, `index`, `len`
* **Aggregations** — `reduce(op=sum|mean|min|max)` with optional **`apply`** (no temp columns needed)
* **Dates** — `date_parse`, `date_format`, `from_timestamp`, `to_timezone`
* **Conditionals** — `if` with predicates `exists|eq|gt|lt|regex`
* **Lookups & merge** — dimension mapping + dict merging
* **UDFs** — safe, registered functions (`udf(name, args...)`)
* **Schema contracts** — optional output validation
* **Tracing** — explain how each value was produced
* **Backends** — default Pandas; swap for Polars/Arrow by implementing a tiny interface
* **Streaming IO** — JSONL → CSV/Parquet with batching
* **Tensors** — convert numeric columns to **PyTorch** tensors (optional)

---

## 30-Second Quickstart

### Requirements

* Python **3.9+**
* `pandas` (required)
* `torch` (optional; for tensors)
* `pyarrow` (optional; Parquet output)

### Install

```bash
git clone https://github.com/alphos-services/orik-tabular-mapping.git
cd orik-tabular-mapping
python -m venv .venv && source .venv/bin/activate
pip install -e .
# optional extras:
# pip install torch pyarrow
```

### Minimal Example

```python
from declarative_converter import DeclarativeConverter

mapping = {
  "columns": {
    "device": {"path": "meta.device_id"},
    "temp_c": {"path": "sensor.temp", "cast": "float", "default": 0.0},
    "stamp": {
      "date_format": {"parse": {"path":"timestamp"}, "fmt": "%Y-%m-%d %H:%M:%S"}
    }
  }
}

data = [
  {"meta":{"device_id":"A-1"},"sensor":{"temp":21.7},"timestamp":"2025-11-11T12:00:00Z"},
  {"meta":{"device_id":"B-2"},"sensor":{"temp":"22.1"},"timestamp":"2025-11-11T12:05:00Z"}
]

df = DeclarativeConverter(mapping).to_dataframe_batch(data)
print(df)
df.to_csv("out.csv", index=False)
```

---

## Developer Quickstart (Builder API)

```python
from declarative_converter import MappingBuilder, Rule, PredicateBuilder as P, DeclarativeConverter

mapping = (
  MappingBuilder()
    .explode("items")                                    # row per item
    .col("user_id").path("user.id").cast("str").end()
    .col("product").rel_path("name").end()
    .col("qty").rel_path("qty").cast("int").default(0).end()
    .col("price").rel_path("price").cast("float").end()
    .col("is_vip").when(P.gt(Rule().path("user.score"), 900), True, False).cast("bool").end()
    # list transform with compact output:
    .col("tags").sort(over=Rule().path("tags"), emit="join", sep=" | ").end()
    # aggregation without temp column:
    .col("total_price").reduce(over=Rule().path("items"), apply=Rule().rel_path("price"), op="sum").end()
    .build()
)

df = DeclarativeConverter(mapping).to_dataframe_single({
  "user":{"id":123,"score":950},
  "items":[{"name":"A","price":3.5,"qty":2},{"name":"B","price":4,"qty":1}],
  "tags":["new","promo"]
})
print(df)
```

---

## Mapping Language (JSON)

### Top-Level

```json
{
  "explode": {"path": "items", "emit_root_when_empty": true},
  "explode_join": {"left": "items", "right": "tags", "how": "inner"},
  "definitions": { "full_name": {"concat":[{"path":"user.first"}," ",{"path":"user.last"}]} },
  "schema": { "columns": { "price": {"type":"float","nullable":false,"min":0} }, "strict": false },
  "columns": { "col_name": { /* rule */ } }
}
```

> Use either `explode` **or** `explode_join` in a mapping.

### Paths

* Dot paths with dict keys & numeric indexes: `user.profile[0].email`
* Wildcards: `items[*].price`
* Filters: `user.emails[?type=="work"]?[0].value`
* Safe index: `?[0]` returns `None` if missing

### Core Rule Operators

* **Values**: `path`, `rel_path`, `const`, `coalesce`
* **Math**: `math: ["add"|"sub"|"mul"|"div", ...]`
* **Strings/Lists**: `concat`, `join`, `index`, `len`
* **Transforms**: `map`, `filter`, `flat_map`, `unique`, `sort`

  * **Output control** via `emit`: `"list"` (default), `"json"`, `"count"`, `"join"` (with `sep`)
  * Optional `limit`
* **Aggregation**: `reduce: { over, op, apply? }` (use `apply` to project element values)
* **Dates**: `date_parse`, `date_format`, `from_timestamp`, `to_timezone`
* **Conditionals**: `if: { cond, then, else }` with predicates `exists|eq|gt|lt|regex`
* **Lookups**: `lookup: { key, table, default }`
* **Merge**: `merge: { objects: [..], strategy: "override"|"first_non_null" }`
* **Macros**: `ref: "definition_name"`
* **UDF**: `udf: { name, args: [...] }`
* **Tail**: `default`, `cast: "str|int|float|bool"`, `on_error: "null|default|raise|warn"`

---

## Advanced Recipes

### Explode & Explode-Join

```python
# explode
MappingBuilder().explode("items") \
  .col("p").rel_path("name").end() \
  .build()

# explode-join (cartesian)
MappingBuilder().explode_join("items", "tags", how="outer") \
  .col("item").rel_path("left.name").end() \
  .col("tag").rel_path("right").end() \
  .build()
```

### List Transforms with Friendly Output

```python
# JSON string
Rule().map(over=Rule().path("user.emails"), apply=Rule().rel_path("value")).build()
# … set emit in builder:
Rule().map(Rule().path("user.emails"), Rule().rel_path("value"), emit="json")

# Count
Rule().filter(Rule().path("items"), where=P.gt(Rule().rel_path("qty"), 0))._merge({"filter":{"emit":"count"}})

# Join with separator
Rule().sort(Rule().path("tags"), emit="join", sep=" | ")
```

### Aggregations that “just work”

```python
# Sum prices directly from items (no intermediate column needed)
Rule().reduce(over=Rule().path("items"), apply=Rule().rel_path("price"), op="sum")
```

### Dates & Timezones

```python
Rule().date_parse(text=Rule().path("created_at"), formats=[], strict=False)
Rule().to_timezone(dt=Rule().path("created_at"), to="Europe/Berlin", from_tz="UTC")
Rule().from_timestamp(sec=Rule().path("ts_ms"), unit="ms")
Rule().date_format(parse=Rule().path("created_at"), fmt="%Y-%m-%d")
```

### Conditionals & Predicates

```python
Rule().when(P.gt(Rule().path("user.score"), 900), then=True, otherwise=False).cast("bool")
```

### Lookups & Merging

```python
Rule().lookup(key=Rule().path("country_code"), table={"DE":"Germany"}, default="Unknown")
Rule().merge(Rule().path("meta"), Rule().const({"version": 3}), strategy="override")
```

### UDFs (User-Defined Functions)

```python
from declarative_converter import register_udf
register_udf("norm_city", lambda s, c: f"{s.strip().title()} ({c})")
Rule().udf("norm_city", Rule().const(" berlin "), Rule().path("country"))
```

### Custom Operations (Plugins)

Add power without touching the engine.

```python
from declarative_converter import register_operation
def _uppercase(rule, ctx, eval_rule, apply_tail_ops):
    v = eval_rule(rule["uppercase"])
    return apply_tail_ops(str(v).upper() if v is not None else None, rule)
register_operation("uppercase", _uppercase)

# mapping: {"columns":{"name_upper":{"uppercase":{"path":"user.first"}}}}
```

### Streaming IO (JSONL → CSV/Parquet)

```python
from declarative_converter.io import stream_jsonl_to_csv, stream_jsonl_to_parquet
conv = DeclarativeConverter(mapping)
stream_jsonl_to_csv(conv, "in.jsonl", "out.csv", batch_size=10_000)
# requires pyarrow:
# stream_jsonl_to_parquet(conv, "in.jsonl", "out.parquet")
```

### Tensor Output

```python
from declarative_converter import DeclarativeConverter
df = DeclarativeConverter(mapping).to_dataframe_single(record)
tensor = DeclarativeConverter(mapping).to_tensor_single(record)  # numeric/bool columns → torch.float32
```

---

## Architecture (Extensible by Design)

* **Engine** — evaluates rules, handles explode, tail ops, errors, schema, tracing
* **Registry** — maps operation name ➜ handler function (`register_operation(...)`)
* **Ops** — built-ins (path, math, string, transforms, dates, reduce, lookup, merge, udf, …)
* **PathResolver** — dotted paths with wildcards/filters/safe index
* **Builder** — fluent `MappingBuilder`/`Rule`/`PredicateBuilder` for delightful DX
* **UDF Registry** — `register_udf(name, fn)` → use from mappings
* **Backends** — default Pandas; implement `DataFrameBackend` to swap

> Contributors can add **new ops** or **new backends** in minutes — no changes to the engine core.

---

## Contributing

We love contributions — from docs to new ops 💚

**Great first issues**

* Add cookbook recipes & tests
* Improve built-in ops coverage
* New list ops (`slice`, `zip`, `window`) or date parsing presets
* Polars or PyArrow backend adapters

**Dev setup**

```bash
git clone https://github.com/alphos-services/orik-tabular-mapping.git
cd orik-tabular-mapping
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"   # if provided; otherwise pip install -e .
pytest -q
```

**PR tips**

* Keep PRs small & focused
* Add tests/docs for new behavior
* Prefer **registry ops** over engine changes
* Be nice. We follow a standard Code of Conduct

If this project helps you, **please ⭐ the repo** and share it — it genuinely boosts adoption and longevity.

---

## Security

If you believe you’ve found a vulnerability:

* **Do not** open a public issue
* Email **[contact@alphos-services.com](mailto:contact@alphos-services.com)** with details & reproduction
* We’ll respond promptly and coordinate a responsible disclosure

---

## License

**MIT License** — see [LICENSE](./LICENSE).

---

## Contact

* General questions & OSS contributions: **[contact@alphos-services.com](mailto:contact@alphos-services.com)**
* Security reports: **[contact@alphos-services.com](mailto:contact@alphos-services.com)**
* Company: **Alphos-Services GmbH**

---

### One last thing 💡

If you reached this far, you probably care about clean data pipelines.
**Star this repository** and help more teams discover a better way to turn **JSON → Tables**.
