# O R I K: Tabular Mapping

> A fully **declarative** (no custom code) protocol and reference implementation for turning **heterogeneous JSON** into **clean tabular datasets** (CSV / DataFrames / PyTorch tensors).
> Built & maintained by **Alphos-Services GmbH**.

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](#license)
[![Python](https://img.shields.io/badge/Python-3.9%2B-blue)](#requirements)

---

##### This project is part of the **ORIK** family of data tools by Alphos-Services GmbH and will be the default handler for external JSON data in the upcoming [ORIK Platform](https://www.alphos-services.com).


---

## Why this project?

Modern data ingestion often means inconsistent JSON from different producers. Writing per-producer ETL code is brittle and hard to maintain.
**ORIK: Tabular Mapping** defines a **JSON-only mapping protocol** and a small Python engine that:

* Maps nested JSON → **flat tables** without custom functions
* Handles **arrays** (row explosion), **type casting**, **defaults**, **aggregations**, **string ops**, **date parsing**, and **conditional logic**
* Produces consistent **CSV**, **Pandas DataFrame**, and optional **PyTorch Tensors**

Perfect for **IoT telemetry**, **e-commerce**, **events**, **timeseries**, **annotations**, and more.

---

## Table of Contents

* [Features](#features)
* [Quickstart](#quickstart)
* [Mapping Protocol](#mapping-protocol)
* [Validation](#validation)
* [ORIK: Platform Integration](#orik-platform-integration)
* [Python API](#python-api)
* [Examples](#examples)
* [Project Structure](#project-structure)
* [Contributing](#contributing)
* [Code of Conduct](#code-of-conduct)
* [Security](#security)
* [Roadmap](#roadmap)
* [License](#license)
* [Contact](#contact)

---

## Features

* **Declarative only**: mappings are pure JSON (storable, lintable, versionable)
* **Row explosion**: turn lists into multiple rows via `explode.path`
* **Robustness**: `cast`, `default`, `coalesce` avoid hard failures
* **Array ops**: `reduce (sum|mean|min|max)`, `join`, `index`, `len`
* **String ops**: `concat`, `join`
* **Math ops**: `add`, `sub`, `mul`, `div`
* **Dates**: `date_format` with optional `fmt_in`
* **Conditionals**: `if` with predicates `exists|eq|gt|lt|regex`
* **Serialization**: `serialize` to embed JSON as strings
* **Works with**: CSV, Pandas, and (optionally) PyTorch tensors

---

## Quickstart

### Requirements

* Python **3.9+**
* `pandas` (required)
* `torch` (optional, for tensor output)

### Installation

```bash
# clone
git clone https://github.com/alphos-services/orik-tabular-mapping.git
cd orik-tabular-mapping

# (recommended) create venv
python -m venv .venv && source .venv/bin/activate

# install lib (editable) + dev extras
pip install -e ".[dev]"
```

> If you don’t use extras, minimally install: `pip install pandas`. For tensors: `pip install torch`.

### Minimal usage

```python
from declarative_converter import DeclarativeConverter, validate_mapping

mapping = {
  "columns": {
    "device": { "path": "meta.device_id" },
    "temperature": { "path": "sensor.temp", "cast": "float", "default": 0.0 },
    "stamp": {
      "date_format": {
        "parse": {"path":"timestamp"},
        "fmt": "%Y-%m-%d %H:%M:%S"
      }
    }
  }
}

data = [
  {"meta":{"device_id":"A-1"},"sensor":{"temp":21.7},"timestamp":"2025-11-11T12:00:00Z"},
  {"meta":{"device_id":"B-2"},"sensor":{"temp":"22.1"},"timestamp":"2025-11-11T12:05:00Z"}
]

ok, errs = validate_mapping(mapping)
if not ok:
    raise RuntimeError("Invalid mapping:\n- " + "\n- ".join(errs))

conv = DeclarativeConverter(mapping)
df = conv.to_dataframe_batch(data)
df.to_csv("out.csv", index=False)
print(df)
```

---

## Mapping Protocol

Top-level structure:

```json
{
  "explode": {
    "path": "sensor.samples",
    "emit_root_when_empty": true
  },
  "columns": {
    "col_name": { /* rule */ }
  }
}
```

### Rule operators (composable)

* **Value access**:

  * `{"path": "root.nested[0].field"}`
  * `{"rel_path": "nested.field"}` // relative to exploded item
  * `{"const": 42}`
  * `{"coalesce": [rule, rule, ...]}`

* **Math**: `{"math": ["add"|"sub"|"mul"|"div", rule, rule, ...]}`

* **Strings**:

  * `{"concat": [rule, {"const": ","}, rule]}`
  * `{"join": {"over": rule, "sep": "|"}}`

* **Arrays**:

  * `{"index": {"of": rule, "at": 0}}`
  * `{"len": rule}`
  * `{"reduce": {"over": rule, "op": "sum|mean|min|max"}}`

* **Dates**:

  * `{"date_format": {"parse": rule, "fmt": "%Y-%m-%d %H:%M:%S", "fmt_in": "%d/%m/%Y %H:%M:%S"}}`

* **Conditionals**:

  * `{"if": {"cond": predicate, "then": rule, "else": rule}}`
  * Predicates:

    * `{"op": "exists", "arg": rule}`
    * `{"op": "eq|gt|lt|regex", "a": rule, "b": rule}`

* **Serialization**:

  * `{"serialize": {"of": rule}}`

* **Tail options** (allowed on any rule):

  * `"default": <any>`
  * `"cast": "str|int|float|bool"`

**Design note:** Columns are evaluated independently (no intra-column references). If you need a reused expression, repeat the rule or post-process.

---

## Validation

Use the provided validator to statically verify mapping structure:

```python
from declarative_converter import validate_mapping, MappingError

ok, errors = validate_mapping(mapping)
if not ok:
    for e in errors:
        print(" -", e)
    raise MappingError("Invalid mapping")
```

* Fails fast on invalid operators/keys
* Precise error paths (e.g., `$.columns.temp.math[1]`)
* Recommended to run in CI for all contributed mappings

---

## ORIK: Platform Integration

Super 🙌 — hier ist eine **erweiterte, GitHub-fertige README-Sektion**,
die du direkt in dein Repository übernehmen kannst.

Sie enthält:

* ✨ **Badges** (PyPI-Version, Build-Status, License, Downloads)
* ⚠️ **Beta-Hinweis** zur ORIK-Plattform
* 🧭 **API-Dokumentation** zum `/otm/validate`-Endpoint
* 💡 **Client Usage Example**

Alles auf **professionellem englischen Niveau**, so wie du es in reifen Open-Source-Projekten (z. B. FastAPI oder OpenAI-SDKs) findest.

---

## 🧩 Orik Tabular Client

> A lightweight Python client for the **ORIK Tabular Mapping API**,  
> providing easy access to data mapping validation and sample transformation endpoints.

### ⚠️ Beta Notice

> **Important:** The **ORIK Platform** and its related APIs are currently in **active development** and part of an **early beta program**.  
> During this phase, API behavior, endpoint availability, and response formats may change without prior notice.  
> Please expect occasional service interruptions or backward-incompatible changes until the platform reaches public release.

---

### 🧭 API Documentation — `/otm/validate`

#### **Endpoint**

`POST /otm/validate`

Validates an ORIK Tabular Mapping definition and optionally converts provided sample data based on that mapping.

#### **Request Body**

| Field               | Type                | Required | Description                                                                 |
| ------------------- | ------------------- | -------- | --------------------------------------------------------------------------- |
| `mapping`           | `object`            | ✅ Yes    | The declarative ORIK mapping definition.                                    |
| `sample_data`       | `object` or `array` | ❌ No     | Example data record(s) to test the mapping.                                 |
| `sample_is_batched` | `boolean`           | ❌ No     | If `true`, `sample_data` is treated as a list of records. Default: `false`. |

#### Example Request

```json
{
  "mapping": {
    "columns": [
      {"source": "name", "target": "Name"},
      {"source": "age", "target": "Age"}
    ]
  },
  "sample_data": {"name": "Alice", "age": 30},
  "sample_is_batched": false
}
```

---

#### **Response**

| Field             | Type            | Description                                               |
| ----------------- | --------------- | --------------------------------------------------------- |
| `is_valid`        | `boolean`       | Whether the mapping is valid.                             |
| `errors`          | `array[string]` | Validation errors, if any.                                |
| `sample_result`   | `object`        | The converted sample data.                                |
| `sample_is_valid` | `boolean`       | Whether the sample conversion succeeded.                  |
| `sample_error`    | `string`        | Message describing the sample conversion result or error. |

#### Example Response

```json
{
  "is_valid": true,
  "errors": [],
  "sample_result": [
    {"Name": "Alice", "Age": 30}
  ],
  "sample_is_valid": true,
  "sample_error": "Input data converted successfully."
}
```

---

#### **Rate Limiting**

The `/otm/validate` endpoint is currently rate-limited to **5 requests per minute per user/IP**.
Clients exceeding this limit will receive a `429 Too Many Requests` response.

---

## Python API

```python
from declarative_converter import DeclarativeConverter

conv = DeclarativeConverter(mapping)

# Single vs Batch
df1 = conv.to_dataframe_single(record)          # one JSON object → DataFrame
dfn = conv.to_dataframe_batch(list_of_records)  # list[JSON] → DataFrame

# Tensors (requires torch)
tensor1 = conv.to_tensor_single(record)
tensorn = conv.to_tensor_batch(list_of_records)
```

**Explode semantics:**
If `explode.path` exists and resolves to an array, **one row per array item** is emitted (with `rel_path` referring to the item). If the array is empty or missing, a **single root row** is emitted when `emit_root_when_empty: true` (default), otherwise the record is skipped.

---

## Examples

See the `/examples` directory with realistic `.py` scripts:

* `example_01_iot_cold_chain.py` — cold-chain sensors (explode + conditions)
* `example_02_ecommerce_orders.py` — e-commerce line items (math + join)
* `example_03_mobility_gps_tracks.py` — mobility GPS points (index + len + reduce)
* `example_04_health_wearables.py` — wearable readings (if + coalesce + date_format)
* `example_05_finance_trades.py` — trades & PnL (math + predicates)
* `example_A..F.py` — additional developer tutorials (schema evolution, inventory, etc.)

Run any example:

```bash
python examples/iot_cold_chain.py
```

---

## Project Structure

```
.
├─ src/
│  ├─ __init__.py                 # exports DeclarativeConverter, validate_mapping, MappingError
│  ├─ converter.py                # core engine
│  └─ validation.py               # mapping validator
├─ examples/
│  ├─ example_01_iot_cold_chain.py
│  ├─ example_02_ecommerce_orders.py
│  ├─ ...
├─ pyproject.toml
├─ README.md
├─ LICENSE
├─ CONTRIBUTING.md
├─ CODE_OF_CONDUCT.md
└─ SECURITY.md
```

---

## Contributing

We welcome issues and PRs!

* **Good first issues**: add example mappings, improve docs, increase operator test coverage
* **Open discussions**: propose new operators or extensions to the protocol
* **Development setup:**

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pre-commit install
pytest -q
```

### Guidelines

* Keep mappings **pure JSON** (no embedded functions or code)
* Add **tests** for new operators / edge-cases
* Update **README** & **examples** when adding features
* Follow Conventional Commits (e.g., `feat:`, `fix:`, `docs:`)
* Target branch: `main`; we squash-merge

---

## Security

If you believe you’ve found a vulnerability:

* **Do not** open a public issue.
* Email us at **[contact@alphos-services.com](mailto:contact@alphos-services.com)** with details and reproduction steps.
* We’ll confirm receipt and work with you on disclosure timelines.

---

## Roadmap

* JSON Schema for mappings and editor autocompletion
* Multi-level array flattening
* Parquet writer & Arrow table output
* Streaming conversion for very large inputs
* Typed column declarations (schema hints)

---

## License

**MIT License** — see [LICENSE](./LICENSE).

---

## Contact

* General questions & OSS contributions: **[contact@alphos-services.com](mailto:contact@alphos-services.com)**
* Security reports: **[contact@alphos-services.com](mailto:contact@alphos-services.com)**
* Company: **Alphos-Services GmbH**
