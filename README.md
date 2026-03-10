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
                                                iceberg_output.py
                                                ┌──────────────────┐
                                                │  pyiceberg        │
                                                │  → Iceberg tables │
                                                │  (Nessie + MinIO) │
                                                └──────────────────┘
                                                               │
                         Jupyter Notebook        Nessie REST    │
                        ┌──────────────────┐    catalog :19120  │
                        │  PyIceberg       │<───────────────────┘
                        │  matplotlib      │    MinIO S3 :9000
                        │  plotly          │
                        └──────────────────┘
```

**Data flow:** MSSQL → `extract.py` → Parquet → dbt/DuckDB → Iceberg (Nessie + MinIO) → Jupyter

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| **Docker** + Docker Compose v2 | Runs MSSQL, Nessie, and MinIO containers |
| **Python 3.11+** | 3.11 recommended (matches CI) |
| **ODBC Driver 18 for SQL Server** | `msodbcsql18` — [install guide](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server) |

> **Note:** The ODBC driver is only needed for the extraction step (`extract.py`). If you skip extraction and use the bundled CSV data, you don't need it.

---

## Quick Start

```bash
# 1. Clone and enter the repo
git clone <repo-url>
cd dbt-duckdb-poc

# 2. Configure environment
cp .env.example .env
# Edit .env — at minimum set a strong SA_PASSWORD and set:
#   MSSQL_DATABASE=lakehouse_source

# 3. Run the full pipeline
make all
# This runs: setup → docker-up → nessie-wait → seed → extract → transform → load-iceberg

# 4. Explore results in Jupyter
source .venv/bin/activate
make notebook
```

### What `make all` does

1. **setup** — creates a Python virtualenv and installs all dependencies
2. **docker-up** — starts three containers: MSSQL 2022, MinIO (S3-compatible storage), and Nessie (Iceberg REST catalog)
3. **nessie-wait** — blocks until the Nessie API is reachable
4. **seed** — waits for MSSQL to be healthy, then runs `scripts/init-db.sql` to create the `lakehouse_source` database with sample data (8 customers, 8 products, 18 orders)
5. **extract** — connects to MSSQL via pyodbc, extracts all tables to `data/parquet/` as Arrow Parquet files
6. **transform** — runs `dbt run` to materialise staging views, enriched marts, and reporting tables into a DuckDB database at `output/analytics.duckdb`
7. **load-iceberg** — reads the dbt output tables from DuckDB and writes them as Iceberg tables to MinIO, catalogued via Nessie's REST API

---

## Environment Configuration

Copy `.env.example` to `.env` and update:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SA_PASSWORD` | ✅ | `YourStrong!Passw0rd` | MSSQL SA password (must meet complexity requirements) |
| `MSSQL_DATABASE` | ✅ | `YourDatabase` | **Set to `lakehouse_source`** — must match `init-db.sql` |
| `MSSQL_SERVER` | | `localhost` | MSSQL hostname |
| `MSSQL_PORT` | | `1433` | MSSQL port |
| `MSSQL_USER` | | `SA` | MSSQL user |
| `MSSQL_PASSWORD` | | same as `SA_PASSWORD` | MSSQL password for extract.py |
| `MSSQL_DRIVER` | | `ODBC Driver 18 for SQL Server` | ODBC driver name |
| `MSSQL_TRUST_CERT` | | `1` | Trust self-signed certs (set for Docker) |

> ⚠️ **Important:** `MSSQL_DATABASE` must be `lakehouse_source` — this is the database name created by `scripts/init-db.sql`.

---

## Docker Services

`docker-compose.yml` defines three services:

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| **mssql** | `mcr.microsoft.com/mssql/server:2022-latest` | 1433 | Source operational database |
| **minio** | `quay.io/minio/minio:latest` | 9000 (API), 9001 (console) | S3-compatible object storage for Iceberg data files |
| **nessie** | `ghcr.io/projectnessie/nessie:latest` | 19120 | Iceberg REST catalog with Git-like branching |

### MinIO credentials

- **Access key:** `minioadmin`
- **Secret key:** `minioadmin`
- **Bucket:** `lakehouse` (created automatically by `iceberg_output.py` via Nessie)
- **Console:** http://localhost:9001

### Nessie configuration

Nessie is configured via environment variables in `docker-compose.yml`:
- Uses in-memory version store (data lost on container restart — fine for a POC)
- Warehouse `warehouse` mapped to `s3://lakehouse/` on MinIO
- S3 credentials passed via the `urn:nessie-secret:quarkus:` indirection pattern
- Authentication disabled

---

## Makefile Targets

| Target | Description |
|--------|-------------|
| `make setup` | Create `.venv` and install all Python dependencies |
| `make docker-up` | Start MSSQL + MinIO + Nessie containers (requires `.env`) |
| `make nessie-wait` | Block until Nessie catalog API is reachable |
| `make seed` | Wait for MSSQL health check, then load seed data |
| `make extract` | Pull MSSQL tables → Parquet files in `data/parquet/` |
| `make transform` | Run `dbt run` to materialise staging, marts, and reporting models |
| `make test` | Run `dbt test` — 26 data tests (uniqueness, not-null) |
| `make load-iceberg` | Export reporting tables to Iceberg via Nessie REST catalog |
| `make notebook` | Launch Jupyter notebook for interactive Iceberg analytics |
| `make test-e2e` | Run full pytest e2e suite (36 tests) |
| `make all` | Full pipeline: setup → docker-up → seed → extract → transform → load-iceberg |
| `make ci` | CI-only pipeline: extract → transform → test (no Iceberg) |
| `make ci-full` | Full CI pipeline including Iceberg + e2e tests |
| `make clean` | Remove `.venv`, `output/`, and Python caches |
| `make docker-down` | Stop and remove Docker containers |

---

## Project Structure

```
dbt-duckdb-poc/
├── .forgejo/workflows/
│   └── ci.yml                       # CI workflow (Forgejo Actions / GitHub Actions compatible)
│
├── data/
│   ├── csv/                         # Original sample CSV files (bundled)
│   └── parquet/                     # Extracted Arrow Parquet files (gitignored)
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml                 # DuckDB profile (output/analytics.duckdb)
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
│   ├── init-db.sql                  # Creates lakehouse_source DB + seed data
│   └── seed.sh                      # Wait-for-healthy + run SQL script
│
├── tests/
│   └── test_e2e.py                  # 37 e2e tests (extraction, dbt, Iceberg, notebook)
│
├── output/                          # Generated artifacts (gitignored)
│   └── analytics.duckdb             # DuckDB database (created by dbt)
│
├── extract.py                       # MSSQL → Arrow Parquet extraction
├── iceberg_output.py                # DuckDB → Iceberg export (via Nessie + MinIO)
├── notebook.ipynb                   # Jupyter notebook — Iceberg analytics
├── docker-compose.yml               # MSSQL + MinIO + Nessie services
├── .env.example                     # Environment variable template
├── requirements.txt                 # pip dependencies
├── pyproject.toml                   # Project metadata + pytest config
├── Makefile
└── README.md
```

---

## Testing

### dbt tests (26 tests)

```bash
make test
```

Tests uniqueness and not-null constraints across all staging, mart, and reporting models.

### End-to-end tests (37 tests)

```bash
make test-e2e
```

The `tests/test_e2e.py` suite covers:

| Test class | What it checks |
|------------|----------------|
| `TestInfrastructure` | MSSQL container health (local only), Nessie API reachability |
| `TestExtraction` | Parquet files exist, row counts, schemas, PK not-null, positive prices |
| `TestDbtTransform` | `dbt run` succeeds, DuckDB file exists, output tables queryable, FK integrity |
| `TestDbtTests` | `dbt test` suite passes (26 data tests) |
| `TestIcebergLoad` | `iceberg_output.py` runs, tables exist in Nessie catalog, round-trip row counts match |
| `TestIcebergDataIntegrity` | Not-null PKs, positive revenues, expected row counts across all 5 Iceberg tables |
| `TestNotebook` | `notebook.ipynb` exists, executes cleanly via `nbconvert --execute` |

> **Note:** `test_mssql_reachable` is skipped in CI (`@pytest.mark.skipif(CI)`) because the MSSQL service container has a runner-assigned name, not `lakehouse-mssql`. MSSQL health is already guaranteed by the CI service healthcheck.

### CI

The Forgejo Actions workflow (`.forgejo/workflows/ci.yml`) runs on every push/PR to `main`:

1. Sets up Python 3.11 + uv for fast dependency installation
2. Starts MSSQL as a service container
3. Downloads and starts MinIO + Nessie as in-container processes
4. Runs `make ci-full` (extract → transform → dbt test → Iceberg load → e2e tests)

---

## Design Decisions

### Why DuckDB?

DuckDB is an in-process analytical database — no server to manage. It reads Parquet natively, integrates with dbt via [dbt-duckdb](https://github.com/duckdb/dbt-duckdb), and runs anywhere Python runs. This makes the entire transformation layer zero-infrastructure: no Spark cluster, no warehouse service, just a single process that handles staging, joins, and aggregations directly on columnar files.

### Why Parquet as an intermediate format?

Parquet provides a clean handoff boundary between extraction and transformation. The `extract.py` step writes schema-embedded, compressed columnar files that any tool in the ecosystem can read (DuckDB, Spark, Pandas, Polars). This decouples the source system from the transformation layer — dbt never connects to MSSQL.

### Why Apache Iceberg + Nessie + MinIO?

Iceberg adds table-level semantics on top of Parquet: ACID transactions, schema evolution, time travel, and partition pruning.

[Apache Nessie](https://projectnessie.org) provides the REST catalog:

- **Standard Iceberg REST spec** — DuckDB, Spark, and Trino can all connect to the same catalog
- **Git-like branching** — create isolated branches for schema experiments without affecting `main`
- **Schema evolution tracking** — every DDL change is versioned alongside the data
- **Single Docker container** — `ghcr.io/projectnessie/nessie` with an in-memory store for the POC

[MinIO](https://min.io) provides S3-compatible object storage, which Nessie uses as the backing store for Iceberg data and metadata files. This mirrors production lakehouse patterns (S3/GCS/ADLS) while running entirely on localhost.

The Jupyter notebook reads Iceberg tables via PyIceberg's `RestCatalog`, proving the open-catalog pattern works end-to-end on a laptop.

---

## Tech Stack

| Tool | Role |
|------|------|
| [MSSQL 2022](https://www.microsoft.com/sql-server) | Source operational database |
| [pyodbc](https://github.com/mkleehammer/pyodbc) + [SQLAlchemy](https://www.sqlalchemy.org) | MSSQL connection & extraction |
| [PyArrow](https://arrow.apache.org/docs/python/) | In-memory columnar format + Parquet I/O |
| [DuckDB](https://duckdb.org) | In-process analytical SQL engine |
| [dbt-core](https://docs.getdbt.com) + [dbt-duckdb](https://github.com/duckdb/dbt-duckdb) | Transformation framework |
| [Apache Iceberg](https://iceberg.apache.org) + [pyiceberg](https://py.iceberg.apache.org) | Open table format |
| [Apache Nessie](https://projectnessie.org) | REST catalog — Git-like branching & multi-engine access |
| [MinIO](https://min.io) | S3-compatible object storage |
| [Jupyter](https://jupyter.org) | Interactive analysis notebooks |
| [matplotlib](https://matplotlib.org) + [plotly](https://plotly.com/python/) | Visualisation |

---

## Troubleshooting

### MSSQL won't start

- Check `SA_PASSWORD` meets [complexity requirements](https://learn.microsoft.com/en-us/sql/relational-databases/security/password-policy)
- Ensure port 1433 isn't already in use: `lsof -i :1433`
- Check container logs: `docker compose logs mssql`

### ODBC driver not found

- Install `msodbcsql18` for your platform — see [Microsoft's guide](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)
- On macOS: `brew install microsoft/mssql-release/msodbcsql18`
- On Ubuntu: follow the apt repo instructions from the guide above

### Nessie / MinIO issues

- Check containers are running: `docker compose ps`
- Check Nessie health: `curl http://localhost:19120/api/v2/config`
- Check MinIO health: `curl http://localhost:9000/minio/health/live`
- MinIO console (browse buckets): http://localhost:9001 (minioadmin/minioadmin)

### extract.py fails with "database not found"

- Ensure `MSSQL_DATABASE=lakehouse_source` in your `.env` file
- Ensure `make seed` ran successfully (creates the database)

---

## License

MIT
