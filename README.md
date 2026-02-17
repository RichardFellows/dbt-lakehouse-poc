# dbt + DuckDB PoC

A proof-of-concept analytics stack using **dbt** with **DuckDB** as the execution engine, demonstrating a full three-layer transformation pipeline from raw CSV data to reporting-ready tables — with a [Marimo](https://marimo.io) notebook for interactive analysis.

---

## What it demonstrates

- **Raw CSV → analytics tables** in a single `dbt run` — no database server required
- A clean **staging → mart → reporting** layered architecture
- DuckDB's `read_csv_auto` as a zero-config data source
- `dbt-duckdb` external materialisation for Parquet export
- dbt schema tests (`unique`, `not_null`) across all layers
- Interactive querying of the output database via a Marimo notebook

---

## Architecture

```
data/csv/
├── customers.csv
├── orders.csv
└── products.csv
        │
        │  DuckDB read_csv_auto
        ▼
┌─────────────────────────────────┐
│         STAGING LAYER           │  (materialized as views)
│  stg_customers                  │
│  stg_orders                     │
│  stg_products                   │
└───────────────┬─────────────────┘
                │  3-way join + computed metrics
                ▼
┌─────────────────────────────────┐
│           MART LAYER            │  (materialized as tables)
│  orders_enriched                │  ← fact table
│  orders_enriched_parquet        │  ← Parquet export
└───────────────┬─────────────────┘
                │  rollups + aggregations
                ▼
┌─────────────────────────────────┐
│        REPORTING LAYER          │  (materialized as tables)
│  rpt_revenue_by_country         │
│  rpt_revenue_by_category        │
│  rpt_customer_summary           │
│  rpt_product_performance        │
└─────────────────────────────────┘
                │
                ▼
  output/analytics.duckdb
  output/orders_enriched.parquet
        │
        ▼
  notebook.py  (Marimo interactive analysis)
```

---

## Prerequisites

- Python 3.11+
- `dbt-duckdb` (`pip install dbt-duckdb`)
- `marimo` (`pip install marimo`)

Or install everything at once:

```bash
pip install -r requirements.txt
```

---

## Quick Start

```bash
# 1. Clone the repo
git clone https://gitlab.com/richard-fellows/dbt-duckdb-poc.git
cd dbt-duckdb-poc

# 2. Create a virtual environment and install dependencies
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 3. Run the dbt pipeline
cd dbt_project
dbt run --profiles-dir .

# 4. Run dbt tests
dbt test --profiles-dir .

# 5. Open the notebook
cd ..
marimo edit notebook.py
```

The notebook connects directly to `output/analytics.duckdb` and queries the reporting tables.

---

## Project Structure

```
dbt-duckdb-poc/
├── data/
│   └── csv/
│       ├── customers.csv        # 10 sample customers
│       ├── orders.csv           # 30 sample orders
│       └── products.csv         # 10 products across 4 categories
│
├── dbt_project/
│   ├── dbt_project.yml          # Project config + materialisation settings
│   ├── profiles.yml             # DuckDB connection profile
│   └── models/
│       ├── staging/
│       │   ├── sources.yml
│       │   ├── schema.yml
│       │   ├── stg_customers.sql
│       │   ├── stg_orders.sql
│       │   └── stg_products.sql
│       ├── marts/
│       │   ├── schema.yml
│       │   ├── orders_enriched.sql
│       │   └── orders_enriched_parquet.sql
│       └── reporting/
│           ├── schema.yml
│           ├── rpt_revenue_by_country.sql
│           ├── rpt_revenue_by_category.sql
│           ├── rpt_customer_summary.sql
│           └── rpt_product_performance.sql
│
├── notebook.py                  # Marimo interactive notebook
├── requirements.txt
└── README.md
```

---

## Layer Descriptions

### Staging
Raw CSV files are ingested using DuckDB's `read_csv_auto`. Each staging model casts columns to appropriate types and renames them for consistency. Staged as **views** (no data duplication).

### Marts
`orders_enriched` joins all three staging models into a single fact table with computed metrics (`line_total`, `days_since_signup`, `order_month`, `order_year`). An external materialisation also exports a Parquet snapshot to `output/`.

### Reporting
Pre-aggregated tables ready for dashboards or direct analysis:

| Model | Description |
|-------|-------------|
| `rpt_revenue_by_country` | Monthly revenue rollup by customer country |
| `rpt_revenue_by_category` | Product category totals — revenue, units, avg order value |
| `rpt_customer_summary` | Lifetime metrics per customer — orders, spend, first/last date |
| `rpt_product_performance` | Sales performance per product including current stock remaining |

---

## Running Tests

```bash
cd dbt_project
dbt test --profiles-dir .
```

26 tests covering `unique` and `not_null` constraints across all layers.

---

## Tech Stack

| Tool | Role |
|------|------|
| [DuckDB](https://duckdb.org) | In-process analytical SQL engine |
| [dbt-core](https://docs.getdbt.com) | Transformation framework |
| [dbt-duckdb](https://github.com/duckdb/dbt-duckdb) | DuckDB adapter for dbt |
| [Marimo](https://marimo.io) | Reactive Python notebook |
