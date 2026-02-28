# dbt-lakehouse-poc

A proof-of-concept **lakehouse pipeline** demonstrating the full journey from an operational MSSQL database through columnar storage, analytical transformation, open-table-format cataloguing, and interactive reporting — all with open-source tooling.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  SOURCE SYSTEM                                                  │
│  MSSQL 2022  (Docker)                                           │
│  operational tables: customers, orders, products                │
└───────────────────────────┬─────────────────────────────────────┘
                            │  pyodbc + SQLAlchemy
                            │  pyarrow extraction
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  LANDING ZONE                                                   │
│  Arrow Parquet files  (data/parquet/)                           │
│  columnar, compressed, schema-embedded                          │
└───────────────────────────┬─────────────────────────────────────┘
                            │  dbt-duckdb reads Parquet natively
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  TRANSFORMATION LAYER  (dbt + DuckDB)                           │
│                                                                 │
│  STAGING   stg_customers · stg_orders · stg_products            │
│     │      (views — type casting, column renaming)              │
│     ▼                                                           │
│  MARTS     orders_enriched  (fact table — 3-way join)           │
│     │      orders_enriched_parquet  (Parquet snapshot)          │
│     ▼                                                           │
│  REPORTING rpt_revenue_by_country · rpt_revenue_by_category     │
│            rpt_customer_summary  · rpt_product_performance      │
└───────────────────────────┬─────────────────────────────────────┘
                            │  pyiceberg
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  OPEN TABLE FORMAT                                              │
│  Apache Iceberg  (catalog: SQLite — iceberg/catalog.db)         │
│  namespace: lakehouse                                           │
│  tables: reporting layer promoted to Iceberg                    │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  ANALYSIS & REPORTING                                           │
│  Jupyter Notebook  — queries Iceberg tables via DuckDB          │
│  matplotlib · plotly visualisations                             │
└─────────────────────────────────────────────────────────────────┘
```

---

## Prerequisites

- Python 3.11+
- Docker + Docker Compose (for MSSQL source)
- ODBC Driver 18 for SQL Server (`msodbcsql18`)

---

## Quick Start

```bash
# 1. Clone the repo
git clone https://gitlab.com/richard-fellows/dbt-duckdb-poc.git
cd dbt-duckdb-poc

# 2. Set up Python environment
make setup
source .venv/bin/activate

# 3. Configure environment variables
cp .env.example .env
# Edit .env if you need a different SA password

# 4. Start MSSQL
make docker-up

# 5. Run the dbt pipeline
cd dbt_project
dbt run --profiles-dir .
dbt test --profiles-dir .
cd ..

# 6. Open the notebook
jupyter notebook
```

---

## Project Structure

```
dbt-lakehouse-poc/
├── data/
│   ├── csv/                         # Original sample CSV files
│   │   ├── customers.csv
│   │   ├── orders.csv
│   │   └── products.csv
│   └── parquet/                     # Extracted Arrow Parquet files (gitignored)
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/                 # Views: type-cast raw sources
│       ├── marts/                   # Tables: enriched fact table + Parquet export
│       └── reporting/               # Tables: pre-aggregated reporting models
│
├── iceberg/                         # Iceberg catalog (gitignored)
│   └── catalog.db                   # SQLite-backed Iceberg catalog
│
├── output/                          # DuckDB database + ad-hoc Parquet exports (gitignored)
│   ├── analytics.duckdb
│   └── orders_enriched.parquet
│
├── notebook.py                      # Marimo interactive notebook (legacy)
├── docker-compose.yml               # MSSQL 2022 service
├── .env.example                     # Environment variable template
├── requirements.txt
├── pyproject.toml
├── Makefile
└── README.md
```

---

## Makefile Targets

| Target | Description |
|--------|-------------|
| `make setup` | Create `.venv` and install all dependencies |
| `make docker-up` | Start MSSQL container (requires `.env`) |
| `make docker-down` | Stop and remove containers |
| `make clean` | Remove `.venv`, `output/`, caches |

---

## Tech Stack

| Tool | Role |
|------|------|
| [MSSQL 2022](https://www.microsoft.com/sql-server) | Source operational database |
| [pyodbc](https://github.com/mkleehammer/pyodbc) + [SQLAlchemy](https://www.sqlalchemy.org) | MSSQL connection & extraction |
| [PyArrow](https://arrow.apache.org/docs/python/) | In-memory columnar format + Parquet I/O |
| [DuckDB](https://duckdb.org) | In-process analytical SQL engine |
| [dbt-core](https://docs.getdbt.com) + [dbt-duckdb](https://github.com/duckdb/dbt-duckdb) | Transformation framework |
| [Apache Iceberg](https://iceberg.apache.org) + [pyiceberg](https://py.iceberg.apache.org) | Open table format & catalog |
| [Jupyter](https://jupyter.org) | Interactive analysis notebooks |
| [matplotlib](https://matplotlib.org) + [plotly](https://plotly.com/python/) | Visualisation |
| [Marimo](https://marimo.io) | Reactive notebook (legacy PoC) |
