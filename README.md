# dbt-lakehouse-poc

A proof-of-concept **lakehouse pipeline** demonstrating the full journey from an operational MSSQL database through columnar storage, analytical transformation, open-table-format cataloguing, and interactive reporting — all with open-source tooling.

---

## Architecture

```
 MSSQL 2022 (Docker)         extract.py           dbt + DuckDB
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────────────────┐
│  customers       │    │                  │    │  STAGING (views)             │
│  orders          │───>│  pyodbc/Arrow    │───>│  stg_customers/orders/prods  │
│  products        │    │  → Parquet files │    │                              │
└──────────────────┘    └──────────────────┘    │  MARTS (tables)              │
                         data/parquet/           │  orders_enriched             │
                                                │                              │
                                                │  REPORTING (tables)          │
                                                │  rpt_revenue_by_country      │
                                                │  rpt_revenue_by_category     │
                                                │  rpt_customer_summary        │
                                                │  rpt_product_performance     │
                                                └──────────────┬───────────────┘
                                                               │
                         Jupyter Notebook        iceberg_output.py
                        ┌──────────────────┐    ┌──────────────────┐
                        │  DuckDB queries  │<───│  pyiceberg        │
                        │  matplotlib      │    │  → Iceberg tables │
                        │  plotly          │    │  (SQLite catalog) │
                        └──────────────────┘    └──────────────────┘
                                                 output/iceberg/
```

**Data flow:** MSSQL → `extract.py` → Parquet → dbt/DuckDB → Iceberg → Jupyter

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| **Docker** + Docker Compose | Runs the MSSQL 2022 source database |
| **Python 3.10+** | 3.11+ recommended |
| **ODBC Driver 18 for SQL Server** | `msodbcsql18` — [install guide](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server) |

---

## Quick Start

```bash
# 1. Clone and enter the repo
git clone https://gitlab.com/richard-fellows/dbt-duckdb-poc.git
cd dbt-duckdb-poc

# 2. Configure environment
cp .env.example .env        # edit SA_PASSWORD if desired

# 3. Run the full pipeline (setup → docker-up → seed → extract → transform → load-iceberg)
make all

# 4. Explore results in Jupyter
source .venv/bin/activate
make notebook
```

---

## Makefile Targets

| Target | Description |
|--------|-------------|
| `make setup` | Create `.venv` and install all Python dependencies |
| `make docker-up` | Start MSSQL container (requires `.env`) |
| `make seed` | Wait for MSSQL health check, then load seed data |
| `make extract` | Pull MSSQL tables → Parquet files in `data/parquet/` |
| `make transform` | Run `dbt run` to materialise staging, marts, and reporting models |
| `make test` | Run `dbt test` against materialised models |
| `make load-iceberg` | Export reporting tables to Apache Iceberg format |
| `make notebook` | Launch Jupyter notebook for interactive analysis |
| `make all` | Full pipeline: setup → docker-up → seed → extract → transform → load-iceberg |
| `make ci` | CI-friendly pipeline: extract → transform → test (assumes MSSQL running) |
| `make clean` | Remove `.venv`, `output/`, and Python caches |
| `make docker-down` | Stop and remove Docker containers |

---

## Project Structure

```
dbt-lakehouse-poc/
├── data/
│   ├── csv/                         # Original sample CSV files
│   └── parquet/                     # Extracted Arrow Parquet files (gitignored)
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/                 # Views: type-cast raw Parquet sources
│       │   ├── stg_customers.sql
│       │   ├── stg_orders.sql
│       │   ├── stg_products.sql
│       │   ├── sources.yml
│       │   └── schema.yml
│       ├── marts/                   # Tables: enriched fact table + Parquet export
│       │   ├── orders_enriched.sql
│       │   ├── orders_enriched_parquet.sql
│       │   └── schema.yml
│       └── reporting/               # Tables: pre-aggregated reporting models
│           ├── rpt_revenue_by_country.sql
│           ├── rpt_revenue_by_category.sql
│           ├── rpt_customer_summary.sql
│           ├── rpt_product_performance.sql
│           └── schema.yml
│
├── scripts/
│   ├── init-db.sql                  # MSSQL schema + seed data
│   └── seed.sh                      # Wait-for-healthy + run SQL script
│
├── output/                          # Generated artifacts (gitignored)
│   └── iceberg/                     # Iceberg tables + SQLite catalog
│
├── extract.py                       # MSSQL → Arrow Parquet extraction
├── iceberg_output.py                # DuckDB → Iceberg export
├── notebook.ipynb                   # Jupyter notebook — Iceberg analytics
├── docker-compose.yml               # MSSQL 2022 service definition
├── .env.example                     # Environment variable template
├── requirements.txt
├── pyproject.toml
├── Makefile
└── README.md
```

---

## Design Decisions

### Why DuckDB?

DuckDB is an in-process analytical database — no server to manage. It reads Parquet natively, integrates with dbt via [dbt-duckdb](https://github.com/duckdb/dbt-duckdb), and runs anywhere Python runs. This makes the entire transformation layer zero-infrastructure: no Spark cluster, no warehouse service, just a single process that handles staging, joins, and aggregations directly on columnar files.

### Why Parquet as an intermediate format?

Parquet provides a clean handoff boundary between extraction and transformation. The `extract.py` step writes schema-embedded, compressed columnar files that any tool in the ecosystem can read (DuckDB, Spark, Pandas, Polars). This decouples the source system from the transformation layer — dbt never connects to MSSQL.

### Why Apache Iceberg?

Iceberg adds table-level semantics on top of Parquet: ACID transactions, schema evolution, time travel, and partition pruning. Using pyiceberg with a lightweight SQLite catalog demonstrates the open table format pattern without requiring a cloud catalog service (AWS Glue, Hive Metastore). The Jupyter notebook reads Iceberg tables directly via DuckDB's `iceberg_scan()`, proving the format works end-to-end.

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
