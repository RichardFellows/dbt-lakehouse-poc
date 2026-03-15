# dbt Lakehouse POC — Demo Script

*A guided walkthrough for presenting the data lakehouse proof of concept. ~15-20 minutes.*

---

## Setup (before the demo)

Ensure the stack is running:

```powershell
.\run.ps1 docker-up         # MSSQL, LocalStack, Nessie
.\run.ps1 setup              # Python venv + dependencies
```

Verify all three containers are healthy:

```powershell
docker compose ps
```

Have two terminals ready:
- **Terminal 1**: for running commands
- **Terminal 2**: `docker compose logs -f` (optional — shows container activity in real time)

---

## Part 1: The Problem Statement (2 min, no commands)

**Talking points:**

> "Most enterprise data pipelines look like this: operational databases feeding directly into reporting tools, or data being manually exported to Excel. There's no versioning, no testing, no reproducibility."
>
> "What we want is a **lakehouse** — a modern architecture that combines the reliability of a data warehouse with the flexibility of a data lake. Open formats, open tools, no vendor lock-in."
>
> "This POC demonstrates the entire journey: operational database → columnar extraction → tested transformations → open table format → self-serve analytics. All open-source, all running locally."

---

## Part 2: The Source System (2 min)

**Seed the database:**

```powershell
.\run.ps1 seed
```

**Show the source data:**

```powershell
docker exec lakehouse-mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P "YourStrong!Passw0rd" -C -Q "SELECT * FROM lakehouse_source.dbo.customers"
```

```powershell
docker exec lakehouse-mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P "YourStrong!Passw0rd" -C -Q "SELECT COUNT(*) AS total_orders FROM lakehouse_source.dbo.orders"
```

**Talking points:**

> "Here's our operational SQL Server — 8 customers, 8 products, 18 orders. Small dataset, but the pattern scales."
>
> "In production this would be STAR, or any MSSQL source. The extraction step doesn't care about the schema — it discovers tables automatically."

---

## Part 3: Extract to Parquet (2 min)

```powershell
.\run.ps1 extract
```

**Show the output:**

```powershell
dir data\parquet
```

**Talking points:**

> "We've just pulled every table from SQL Server into **Apache Parquet** — a columnar, compressed, schema-embedded format."
>
> "Why Parquet? It's the lingua franca of the data ecosystem. DuckDB, Spark, Pandas, Polars — everything reads it. And it decouples us from the source system. dbt never touches SQL Server directly."
>
> "This is your **clean handoff boundary**. The extract step could run on a schedule — nightly, hourly, whatever suits the use case."

---

## Part 4: Transform with dbt (3 min)

```powershell
.\run.ps1 transform
```

**Show the dbt output (model summary).**

**Optionally open a model file to show the SQL:**

Open `dbt_project/models/marts/orders_enriched.sql` in your editor.

**Talking points:**

> "dbt is the transformation layer. It takes those raw Parquet files and builds a clean, tested analytical model."
>
> "Three layers:"
> - **Staging** — views that type-cast and clean the raw data. No business logic, just shaping.
> - **Marts** — the enriched fact table. Orders joined with customers and products, with computed metrics like line totals and days since signup.
> - **Reporting** — pre-aggregated tables: revenue by country, revenue by category, customer summaries, product performance.
>
> "Everything is SQL. Version-controlled. Reviewable in a PR. No black-box ETL tools."
>
> "The execution engine is **DuckDB** — an in-process analytical database. No server to manage, no cluster to provision. It just runs."

---

## Part 5: Test the Data (2 min)

```powershell
.\run.ps1 test
```

**Talking points:**

> "26 data quality tests just ran — uniqueness constraints, not-null checks, referential integrity."
>
> "This is your **data contract layer**. If someone changes the source schema, or bad data sneaks in, the pipeline catches it here — not in a dashboard three weeks later."
>
> "These tests are defined in YAML alongside the models. Same repo, same PR, same review process as the transformation logic."

---

## Part 6: Load to Iceberg (3 min)

```powershell
.\run.ps1 load-iceberg
```

**Verify the catalog:**

```powershell
Invoke-RestMethod http://localhost:19120/api/v2/trees/main | ConvertTo-Json -Depth 5
```

**Talking points:**

> "Now we're writing the output into **Apache Iceberg** — an open table format that adds warehouse-grade features on top of Parquet."
>
> "What does Iceberg give us?"
> - **ACID transactions** — no partial writes, no corrupted state
> - **Schema evolution** — add/rename/drop columns without rewriting data
> - **Time travel** — query any previous version of a table
> - **Partition pruning** — only read the data you need
>
> "The catalog is **Apache Nessie** — it's like Git for your data catalog. You can create branches, experiment with schema changes, and merge when you're ready."
>
> "Storage is S3-compatible (LocalStack here, would be actual S3 or ADLS in production). The data files are just Parquet under the hood — Iceberg adds the metadata layer."
>
> "This is the **open lakehouse pattern**: any tool that speaks the Iceberg REST catalog spec can read this data. DuckDB, Spark, Trino, Flink — no vendor lock-in."

---

## Part 7: Interactive Analytics (3 min)

```powershell
.\run.ps1 notebook
```

Walk through the Jupyter notebook cells:

1. **Catalog connection** — show how PyIceberg connects to Nessie's REST API
2. **Schema inspection** — query table schemas without reading data
3. **Time travel** — list snapshot history for each table
4. **Revenue charts** — matplotlib/plotly visualisations (revenue by country, by category)
5. **Customer summary** — top customers by spend

**Talking points:**

> "Here's the payoff. An analyst can connect to the Iceberg catalog and query directly — no ETL team in the loop, no waiting for a data extract."
>
> "We can inspect schemas, browse snapshot history, and query the actual data — all through standard Python libraries."
>
> "The charts here are simple examples, but the point is: **the data is accessible in an open format through a standard API**. You could just as easily point Power BI, Tableau, or a Spark cluster at the same catalog."

---

## Part 8: The Full Picture (2 min, no commands)

**Architecture recap:**

```
MSSQL (source) → Parquet (extract) → dbt/DuckDB (transform + test) → Iceberg/Nessie (catalog) → Jupyter (analytics)
```

**Talking points:**

> "Let's zoom out. What we've built is:"
>
> 1. **Extraction** that decouples us from the source system
> 2. **Transformations** that are version-controlled, tested, and reviewable
> 3. **An open table format** that any tool can read — no lock-in
> 4. **A catalog** that tracks schema evolution and supports branching
> 5. **Self-serve analytics** without needing a dedicated warehouse
>
> "The entire stack is open-source. It runs on a laptop. There are no cloud dependencies, no licence costs, no proprietary formats."
>
> "For production, you'd swap LocalStack for real S3/ADLS, add scheduling (Airflow, cron, CI), and point your BI tools at the Nessie catalog. The architecture stays the same."

---

## Bonus: CI Pipeline (if time allows)

Show `.github/workflows/ci.yml`:

> "The entire pipeline runs in CI on every push. MSSQL, LocalStack, and Nessie spin up as containers. Extract, transform, test, load — all automated. 37 end-to-end tests validate the full round trip."

```powershell
.\run.ps1 test-e2e
```

---

## Common Questions

**"Why not just use Spark?"**
> DuckDB handles analytical workloads without cluster overhead. For datasets that fit on a single machine (which is most enterprise datasets), it's faster to set up and cheaper to run. The Iceberg output is Spark-compatible if you need to scale later.

**"How does this compare to Databricks / Snowflake / Synapse?"**
> Same lakehouse concept, but fully open-source and self-hosted. No vendor lock-in on the storage format (Iceberg) or catalog (Nessie). You could migrate to any of those platforms later because everything is in open formats.

**"What about incremental loads?"**
> dbt supports incremental materialisation. The extract step could be modified to pull only changed records (CDC). Iceberg handles upserts natively. This POC uses full refreshes for simplicity.

**"Can Power BI / Tableau connect to this?"**
> Yes — either via DuckDB (ODBC/JDBC) for the transformed tables, or via Trino/Spark connected to the Nessie catalog for Iceberg tables. The open catalog spec means any compatible query engine can read the data.

**"What about data governance / lineage?"**
> dbt generates a lineage graph (`dbt docs generate` then `dbt docs serve`). Nessie tracks every schema change with Git-like versioning. For full governance you'd add a tool like DataHub or OpenMetadata on top.
