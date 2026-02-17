import marimo

__generated_with = "0.10.0"
app = marimo.App(width="medium", app_title="dbt + DuckDB Orders Analysis")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import duckdb
    from pathlib import Path
    return mo, pd, duckdb, Path


@app.cell
def _(mo):
    mo.md(
        """
        # 📦 dbt + DuckDB Orders Analysis

        This notebook reads from the **DuckDB database** produced by `dbt run`, querying both
        the enriched mart and the pre-built reporting tables.

        > Run `dbt run --profiles-dir .` in the `dbt_project/` directory first.
        """
    )
    return


@app.cell
def _(Path, duckdb, mo):
    db_path = Path(__file__).parent / "output" / "analytics.duckdb"

    if not db_path.exists():
        mo.stop(
            True,
            mo.callout(
                mo.md(f"❌ DuckDB file not found at `{db_path}`.\n\nRun `dbt run --profiles-dir .` from the `dbt_project/` directory first!"),
                kind="danger",
            ),
        )

    con = duckdb.connect(str(db_path), read_only=True)

    row_count = con.execute("SELECT COUNT(*) FROM orders_enriched").fetchone()[0]
    mo.callout(
        mo.md(f"✅ Connected to `{db_path.name}` — **{row_count:,} rows** in `orders_enriched`"),
        kind="success",
    )
    return con, db_path, row_count


@app.cell
def _(con, mo):
    mo.md("## 🔍 Raw Data Preview — `orders_enriched`")
    df_raw = con.execute("SELECT * FROM orders_enriched LIMIT 18").df()
    mo.ui.table(df_raw, label="orders_enriched (first 18 rows)", selection=None)
    return (df_raw,)


@app.cell
def _(con, mo):
    # Summary stats from reporting tables
    stats = con.execute("""
        SELECT
            SUM(total_orders)   AS total_orders,
            SUM(total_spend)    AS total_revenue,
            AVG(avg_order_value) AS avg_order_value,
            COUNT(*)            AS num_customers
        FROM rpt_customer_summary
    """).fetchone()

    top_customer = con.execute("""
        SELECT customer_name FROM rpt_customer_summary ORDER BY total_spend DESC LIMIT 1
    """).fetchone()[0]

    top_product = con.execute("""
        SELECT product_name FROM rpt_product_performance ORDER BY total_revenue DESC LIMIT 1
    """).fetchone()[0]

    mo.md(f"""
    ## 📊 Key Metrics

    | Metric | Value |
    |--------|-------|
    | Total orders | **{int(stats[0]):,}** |
    | Total revenue | **£{stats[1]:,.2f}** |
    | Average order value | **£{stats[2]:,.2f}** |
    | Total customers | **{stats[3]:,}** |
    | Top customer (by spend) | **{top_customer}** |
    | Top product (by revenue) | **{top_product}** |
    """)
    return stats, top_customer, top_product


@app.cell
def _(mo):
    mo.md("## 🌍 Revenue by Country — `rpt_revenue_by_country`")
    return


@app.cell
def _(con, mo):
    df_country = con.execute("""
        SELECT
            country,
            SUM(num_orders)     AS total_orders,
            SUM(units_sold)     AS total_units,
            SUM(total_revenue)  AS total_revenue,
            AVG(avg_order_value)::DECIMAL(10,2) AS avg_order_value
        FROM rpt_revenue_by_country
        GROUP BY country
        ORDER BY total_revenue DESC
    """).df()
    mo.ui.table(df_country, label="Revenue by Country (aggregated)", selection=None)
    return (df_country,)


@app.cell
def _(con, mo):
    mo.md("## 📅 Monthly Revenue Trends — `rpt_revenue_by_country`")
    df_monthly = con.execute("""
        SELECT
            year,
            month,
            SUM(num_orders)         AS total_orders,
            SUM(total_revenue)      AS total_revenue
        FROM rpt_revenue_by_country
        GROUP BY year, month
        ORDER BY year, month
    """).df()
    mo.ui.table(df_monthly, label="Monthly Revenue", selection=None)
    return (df_monthly,)


@app.cell
def _(mo):
    mo.md("## 📦 Revenue by Product Category — `rpt_revenue_by_category`")
    return


@app.cell
def _(con, mo):
    df_category = con.execute("SELECT * FROM rpt_revenue_by_category").df()
    mo.ui.table(df_category, label="Revenue by Category", selection=None)
    return (df_category,)


@app.cell
def _(mo):
    mo.md("## 👤 Customer Lifetime Summary — `rpt_customer_summary`")
    return


@app.cell
def _(con, mo):
    df_customers = con.execute("SELECT * FROM rpt_customer_summary ORDER BY total_spend DESC").df()
    mo.ui.table(df_customers, label="Customer Summary", selection=None)
    return (df_customers,)


@app.cell
def _(mo):
    mo.md("## 🛒 Product Performance — `rpt_product_performance`")
    return


@app.cell
def _(con, mo):
    df_products = con.execute("SELECT * FROM rpt_product_performance ORDER BY total_revenue DESC").df()
    mo.ui.table(df_products, label="Product Performance (with stock remaining)", selection=None)
    return (df_products,)


@app.cell
def _(mo):
    mo.md("""
    ---
    ## 🔧 Architecture

    ```
    CSV files (customers, orders, products)
         ↓  [DuckDB read_csv_auto]
    dbt staging layer   (stg_customers, stg_orders, stg_products)  [views]
         ↓  [dbt mart model]
    orders_enriched     (3-way join, computed metrics)              [table]
         ↓  [dbt reporting models]
    rpt_revenue_by_country    — monthly revenue by country         [table]
    rpt_revenue_by_category   — category performance               [table]
    rpt_customer_summary      — lifetime customer metrics          [table]
    rpt_product_performance   — product sales + stock              [table]
         ↓
    output/analytics.duckdb   (all tables queryable)
    output/orders_enriched.parquet  (portable snapshot)
    ```
    """)
    return


if __name__ == "__main__":
    app.run()
