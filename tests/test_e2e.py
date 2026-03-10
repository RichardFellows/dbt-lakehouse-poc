"""
End-to-end pipeline tests: MSSQL → extract → dbt/DuckDB → Iceberg (Nessie) → query.

Prerequisites (must be running before this suite):
    make docker-up   # starts MSSQL + Nessie containers
    make seed        # loads init-db.sql data

Run with:
    make test-e2e
    # or directly:
    pytest tests/test_e2e.py -v

The suite runs the full pipeline in order (pytest ordering is top-down) and
validates each stage's outputs before proceeding to the next.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PARQUET_DIR = PROJECT_ROOT / "data" / "parquet"
DBT_DIR = PROJECT_ROOT / "dbt_project"
OUTPUT_DIR = PROJECT_ROOT / "output"
DUCKDB_PATH = PROJECT_ROOT / "output" / "analytics.duckdb"
ICEBERG_WAREHOUSE = OUTPUT_DIR / "iceberg" / "warehouse"
NESSIE_URL = os.environ.get("NESSIE_URL", "http://localhost:19120")
NOTEBOOK_PATH = PROJECT_ROOT / "notebook.ipynb"

# Seed data row counts (must match scripts/init-db.sql)
EXPECTED_ROW_COUNTS = {
    "customers": 8,
    "products": 8,
    "orders": 18,
}

# All dbt output tables written to Iceberg
ICEBERG_TABLES = [
    "orders_enriched",
    "rpt_revenue_by_country",
    "rpt_revenue_by_category",
    "rpt_customer_summary",
    "rpt_product_performance",
]

# Expected minimum row counts per dbt output table
# (these are lower bounds — the seed data is fixed so they can be exact)
EXPECTED_DBT_ROWS = {
    "orders_enriched": 18,          # one row per order
    "rpt_revenue_by_country": 1,    # at least one country/month combo
    "rpt_revenue_by_category": 1,   # at least one category
    "rpt_customer_summary": 8,      # one row per customer
    "rpt_product_performance": 8,   # one row per product (that had sales)
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run(cmd: list[str], cwd: Path | None = None, env: dict | None = None) -> subprocess.CompletedProcess:
    """Run *cmd* and return the CompletedProcess; fail the test on non-zero exit."""
    result = subprocess.run(
        cmd,
        cwd=str(cwd or PROJECT_ROOT),
        capture_output=True,
        text=True,
        env={**os.environ, **(env or {})},
    )
    return result


def _venv_python() -> str:
    """Return the venv Python path if present, else the current interpreter."""
    venv_py = PROJECT_ROOT / ".venv" / "bin" / "python"
    return str(venv_py) if venv_py.exists() else sys.executable


def _venv_bin(name: str) -> str:
    """Return path to a binary in .venv/bin/, falling back to PATH."""
    venv_path = PROJECT_ROOT / ".venv" / "bin" / name
    return str(venv_path) if venv_path.exists() else name


def _nessie_url() -> str:
    return NESSIE_URL.rstrip("/")


def _latest_metadata(table_name: str) -> Path:
    """Return the path to the latest Iceberg metadata JSON for *table_name*."""
    meta_dir = ICEBERG_WAREHOUSE / "default" / table_name / "metadata"
    files = sorted(meta_dir.glob("*.metadata.json"))
    assert files, f"No metadata files found in {meta_dir}"
    return files[-1]


# ---------------------------------------------------------------------------
# Stage 0 — Infrastructure health checks
# ---------------------------------------------------------------------------

class TestInfrastructure:
    """Verify Docker services are reachable before running the pipeline."""

    def test_mssql_reachable(self):
        """MSSQL container should be healthy (docker inspect)."""
        result = _run(
            ["docker", "inspect", "--format={{.State.Health.Status}}", "lakehouse-mssql"]
        )
        assert result.returncode == 0, (
            "Could not inspect 'lakehouse-mssql' container — is 'make docker-up' running?\n"
            + result.stderr
        )
        status = result.stdout.strip()
        assert status == "healthy", (
            f"MSSQL container health status is '{status}', expected 'healthy'.\n"
            "Wait for the container to become healthy and retry."
        )

    def test_nessie_reachable(self):
        """Nessie REST API should respond at /api/v2/config."""
        import urllib.request
        import urllib.error

        url = f"{_nessie_url()}/api/v2/config"
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                assert resp.status == 200, f"Nessie returned HTTP {resp.status}"
        except urllib.error.URLError as exc:
            pytest.fail(
                f"Cannot reach Nessie at {url}: {exc}\n"
                "Run 'make docker-up' and wait for Nessie to be healthy."
            )


# ---------------------------------------------------------------------------
# Stage 1 — Extraction (MSSQL → Parquet)
# ---------------------------------------------------------------------------

class TestExtraction:
    """Run extract.py and validate the output Parquet files."""

    def test_extract_runs_successfully(self):
        """extract.py should exit 0."""
        result = _run([_venv_python(), "extract.py"])
        assert result.returncode == 0, (
            "extract.py failed:\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    def test_parquet_files_exist(self):
        """All three source tables should produce Parquet files."""
        for table in EXPECTED_ROW_COUNTS:
            parquet_file = PARQUET_DIR / f"{table}.parquet"
            assert parquet_file.exists(), (
                f"Expected Parquet file not found: {parquet_file}\n"
                "Run 'make extract' to produce it."
            )

    def test_parquet_row_counts(self):
        """Each Parquet file must contain exactly the seeded row count."""
        for table, expected_rows in EXPECTED_ROW_COUNTS.items():
            parquet_file = PARQUET_DIR / f"{table}.parquet"
            pf = pq.read_table(parquet_file)
            actual_rows = len(pf)
            assert actual_rows == expected_rows, (
                f"{table}.parquet: expected {expected_rows} rows, got {actual_rows}"
            )

    def test_parquet_schemas(self):
        """Each Parquet file must contain the expected columns."""
        expected_columns = {
            "customers": {"customer_id", "customer_name", "email", "country", "signup_date"},
            "products": {"product_id", "product_name", "category", "unit_price", "stock_qty"},
            "orders": {"order_id", "customer_id", "product_id", "quantity", "order_date", "status"},
        }
        for table, cols in expected_columns.items():
            parquet_file = PARQUET_DIR / f"{table}.parquet"
            pf = pq.read_table(parquet_file)
            actual_cols = set(pf.schema.names)
            missing = cols - actual_cols
            assert not missing, (
                f"{table}.parquet is missing columns: {missing}\n"
                f"Actual columns: {actual_cols}"
            )

    def test_customers_no_null_pks(self):
        """customers.parquet must have no null customer_id values."""
        pf = pq.read_table(PARQUET_DIR / "customers.parquet", columns=["customer_id"])
        null_count = pf.column("customer_id").null_count
        assert null_count == 0, f"customers.parquet has {null_count} null customer_id(s)"

    def test_products_no_null_pks(self):
        """products.parquet must have no null product_id values."""
        pf = pq.read_table(PARQUET_DIR / "products.parquet", columns=["product_id"])
        null_count = pf.column("product_id").null_count
        assert null_count == 0, f"products.parquet has {null_count} null product_id(s)"

    def test_orders_no_null_pks(self):
        """orders.parquet must have no null order_id values."""
        pf = pq.read_table(PARQUET_DIR / "orders.parquet", columns=["order_id"])
        null_count = pf.column("order_id").null_count
        assert null_count == 0, f"orders.parquet has {null_count} null order_id(s)"

    def test_products_positive_prices(self):
        """All product unit_prices must be > 0."""
        import pyarrow.compute as pc
        pf = pq.read_table(PARQUET_DIR / "products.parquet", columns=["unit_price"])
        prices = pf.column("unit_price")
        non_positive = pc.sum(pc.cast(pc.less_equal(prices, 0), "int64")).as_py()
        assert non_positive == 0, f"{non_positive} products have non-positive unit_price"


# ---------------------------------------------------------------------------
# Stage 2 — dbt transform
# ---------------------------------------------------------------------------

class TestDbtTransform:
    """Run dbt models and validate materialisation."""

    def test_dbt_run_succeeds(self):
        """'dbt run' must exit 0 with all models materialised."""
        result = _run(
            [_venv_bin("dbt"), "run", "--profiles-dir", "."],
            cwd=DBT_DIR,
        )
        assert result.returncode == 0, (
            "dbt run failed:\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )
        # Check that no model reported an error in the output
        assert "ERROR" not in result.stdout or "0 errors" in result.stdout, (
            f"dbt reported model errors:\n{result.stdout}"
        )

    def test_duckdb_file_exists(self):
        """The dbt DuckDB database file must exist after 'dbt run'."""
        assert DUCKDB_PATH.exists(), (
            f"DuckDB database not found at {DUCKDB_PATH}\n"
            "Run 'make transform' to produce it."
        )

    def test_dbt_output_tables_exist(self):
        """All expected dbt output tables must be queryable in DuckDB."""
        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        try:
            for table in ICEBERG_TABLES:
                result = con.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()
                assert result is not None, f"Table '{table}' not found in DuckDB"
                row_count = result[0]
                min_rows = EXPECTED_DBT_ROWS[table]
                assert row_count >= min_rows, (
                    f"Table '{table}': expected at least {min_rows} rows, got {row_count}"
                )
        finally:
            con.close()

    def test_orders_enriched_no_null_order_ids(self):
        """orders_enriched must have no null order_id values (PK integrity)."""
        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        try:
            result = con.execute(
                "SELECT COUNT(*) FROM orders_enriched WHERE order_id IS NULL"
            ).fetchone()
            assert result[0] == 0, f"orders_enriched has {result[0]} null order_id(s)"
        finally:
            con.close()

    def test_orders_enriched_positive_line_total(self):
        """Every row in orders_enriched must have line_total > 0."""
        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        try:
            result = con.execute(
                "SELECT COUNT(*) FROM orders_enriched WHERE line_total <= 0 OR line_total IS NULL"
            ).fetchone()
            assert result[0] == 0, (
                f"orders_enriched has {result[0]} row(s) with line_total <= 0 or NULL"
            )
        finally:
            con.close()

    def test_orders_enriched_fk_integrity(self):
        """Every order must have a valid customer and product (no orphaned joins)."""
        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        try:
            result = con.execute(
                """
                SELECT COUNT(*) FROM orders_enriched
                WHERE customer_id IS NULL OR product_id IS NULL
                """
            ).fetchone()
            assert result[0] == 0, (
                f"orders_enriched has {result[0]} row(s) with null customer_id or product_id"
            )
        finally:
            con.close()

    def test_rpt_customer_summary_total_spend_positive(self):
        """All customers in rpt_customer_summary must have total_spend > 0."""
        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        try:
            result = con.execute(
                "SELECT COUNT(*) FROM rpt_customer_summary WHERE total_spend <= 0 OR total_spend IS NULL"
            ).fetchone()
            assert result[0] == 0, (
                f"rpt_customer_summary has {result[0]} customer(s) with total_spend <= 0"
            )
        finally:
            con.close()

    def test_rpt_revenue_by_category_no_null_revenue(self):
        """All categories must have non-null, positive total_revenue."""
        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        try:
            result = con.execute(
                "SELECT COUNT(*) FROM rpt_revenue_by_category WHERE total_revenue IS NULL OR total_revenue <= 0"
            ).fetchone()
            assert result[0] == 0, (
                f"rpt_revenue_by_category has {result[0]} row(s) with null/zero revenue"
            )
        finally:
            con.close()


# ---------------------------------------------------------------------------
# Stage 3 — dbt tests
# ---------------------------------------------------------------------------

class TestDbtTests:
    """Run the built-in dbt schema tests."""

    def test_dbt_test_suite_passes(self):
        """'dbt test' must exit 0 — all schema tests pass."""
        result = _run(
            [_venv_bin("dbt"), "test", "--profiles-dir", "."],
            cwd=DBT_DIR,
        )
        assert result.returncode == 0, (
            "dbt test failed:\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )


# ---------------------------------------------------------------------------
# Stage 4 — Iceberg load
# ---------------------------------------------------------------------------

class TestIcebergLoad:
    """Run iceberg_output.py and validate round-trip row counts."""

    def test_iceberg_output_runs_successfully(self):
        """iceberg_output.py should exit 0."""
        result = _run(
            [_venv_python(), "iceberg_output.py"],
        )
        assert result.returncode == 0, (
            "iceberg_output.py failed:\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

    def test_iceberg_metadata_files_exist(self):
        """Each Iceberg table must have at least one metadata.json file."""
        for table in ICEBERG_TABLES:
            meta_dir = ICEBERG_WAREHOUSE / "default" / table / "metadata"
            assert meta_dir.exists(), (
                f"Iceberg metadata directory not found: {meta_dir}"
            )
            metadata_files = list(meta_dir.glob("*.metadata.json"))
            assert metadata_files, (
                f"No metadata JSON files found for table '{table}' in {meta_dir}"
            )

    def test_iceberg_data_files_exist(self):
        """Each Iceberg table must have at least one data (Parquet) file."""
        for table in ICEBERG_TABLES:
            data_dir = ICEBERG_WAREHOUSE / "default" / table / "data"
            assert data_dir.exists(), (
                f"Iceberg data directory not found: {data_dir}"
            )
            data_files = list(data_dir.glob("*.parquet"))
            assert data_files, (
                f"No Parquet data files found for table '{table}' in {data_dir}"
            )

    def test_iceberg_round_trip_row_counts(self):
        """Row counts from DuckDB iceberg_scan must match dbt output table counts."""
        # Get expected counts from DuckDB
        con_db = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        expected_counts: dict[str, int] = {}
        try:
            for table in ICEBERG_TABLES:
                result = con_db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                expected_counts[table] = result[0]
        finally:
            con_db.close()

        # Verify each table via iceberg_scan
        con_ice = duckdb.connect()
        con_ice.install_extension("iceberg")
        con_ice.load_extension("iceberg")
        try:
            for table in ICEBERG_TABLES:
                metadata_file = _latest_metadata(table)
                result = con_ice.execute(
                    f"SELECT COUNT(*) FROM iceberg_scan('{metadata_file}')"
                ).fetchone()
                actual_count = result[0]
                expected = expected_counts[table]
                assert actual_count == expected, (
                    f"Iceberg round-trip mismatch for '{table}': "
                    f"DuckDB had {expected} rows, iceberg_scan returned {actual_count}"
                )
        finally:
            con_ice.close()


# ---------------------------------------------------------------------------
# Stage 5 — Iceberg data integrity via DuckDB iceberg_scan
# ---------------------------------------------------------------------------

class TestIcebergDataIntegrity:
    """Query Iceberg tables via DuckDB iceberg_scan and assert data invariants."""

    @pytest.fixture(scope="class")
    def ice_con(self):
        con = duckdb.connect()
        con.install_extension("iceberg")
        con.load_extension("iceberg")
        yield con
        con.close()

    def _scan(self, con: duckdb.DuckDBPyConnection, table: str, query_suffix: str = "") -> list:
        metadata_file = _latest_metadata(table)
        sql = f"SELECT * FROM iceberg_scan('{metadata_file}'){query_suffix}"
        return con.execute(sql).fetchall()

    def _count(self, con: duckdb.DuckDBPyConnection, table: str, where: str = "") -> int:
        metadata_file = _latest_metadata(table)
        where_clause = f" WHERE {where}" if where else ""
        sql = f"SELECT COUNT(*) FROM iceberg_scan('{metadata_file}'){where_clause}"
        return con.execute(sql).fetchone()[0]

    def test_orders_enriched_no_null_pks(self, ice_con):
        """orders_enriched Iceberg table: no null order_ids."""
        nulls = self._count(ice_con, "orders_enriched", "order_id IS NULL")
        assert nulls == 0, f"orders_enriched (Iceberg): {nulls} null order_id(s)"

    def test_orders_enriched_revenue_positive(self, ice_con):
        """orders_enriched Iceberg table: all line_totals > 0."""
        bad = self._count(ice_con, "orders_enriched", "line_total <= 0 OR line_total IS NULL")
        assert bad == 0, f"orders_enriched (Iceberg): {bad} row(s) with line_total <= 0"

    def test_orders_enriched_fk_not_null(self, ice_con):
        """orders_enriched Iceberg table: customer_id and product_id not null."""
        bad = self._count(
            ice_con, "orders_enriched",
            "customer_id IS NULL OR product_id IS NULL"
        )
        assert bad == 0, f"orders_enriched (Iceberg): {bad} orphaned FK row(s)"

    def test_orders_enriched_expected_row_count(self, ice_con):
        """orders_enriched Iceberg table: exactly 18 rows (one per seed order)."""
        count = self._count(ice_con, "orders_enriched")
        assert count == 18, f"orders_enriched (Iceberg): expected 18 rows, got {count}"

    def test_rpt_customer_summary_no_null_ids(self, ice_con):
        """rpt_customer_summary Iceberg table: no null customer_id values."""
        nulls = self._count(ice_con, "rpt_customer_summary", "customer_id IS NULL")
        assert nulls == 0, f"rpt_customer_summary (Iceberg): {nulls} null customer_id(s)"

    def test_rpt_customer_summary_positive_spend(self, ice_con):
        """rpt_customer_summary Iceberg table: all total_spend > 0."""
        bad = self._count(ice_con, "rpt_customer_summary", "total_spend <= 0 OR total_spend IS NULL")
        assert bad == 0, f"rpt_customer_summary (Iceberg): {bad} customer(s) with total_spend <= 0"

    def test_rpt_customer_summary_row_count(self, ice_con):
        """rpt_customer_summary Iceberg table: one row per seeded customer (8)."""
        count = self._count(ice_con, "rpt_customer_summary")
        assert count == 8, f"rpt_customer_summary (Iceberg): expected 8 rows, got {count}"

    def test_rpt_revenue_by_category_no_null_revenue(self, ice_con):
        """rpt_revenue_by_category Iceberg table: total_revenue not null and > 0."""
        bad = self._count(
            ice_con, "rpt_revenue_by_category",
            "total_revenue IS NULL OR total_revenue <= 0"
        )
        assert bad == 0, f"rpt_revenue_by_category (Iceberg): {bad} row(s) with null/zero revenue"

    def test_rpt_revenue_by_category_no_null_category(self, ice_con):
        """rpt_revenue_by_category Iceberg table: category column not null."""
        nulls = self._count(ice_con, "rpt_revenue_by_category", "category IS NULL")
        assert nulls == 0, f"rpt_revenue_by_category (Iceberg): {nulls} null category value(s)"

    def test_rpt_product_performance_no_null_ids(self, ice_con):
        """rpt_product_performance Iceberg table: no null product_id values."""
        nulls = self._count(ice_con, "rpt_product_performance", "product_id IS NULL")
        assert nulls == 0, f"rpt_product_performance (Iceberg): {nulls} null product_id(s)"

    def test_rpt_product_performance_positive_revenue(self, ice_con):
        """rpt_product_performance Iceberg table: all products have total_revenue > 0."""
        bad = self._count(
            ice_con, "rpt_product_performance",
            "total_revenue IS NULL OR total_revenue <= 0"
        )
        assert bad == 0, f"rpt_product_performance (Iceberg): {bad} product(s) with no revenue"

    def test_rpt_revenue_by_country_not_null_country(self, ice_con):
        """rpt_revenue_by_country Iceberg table: country column not null."""
        nulls = self._count(ice_con, "rpt_revenue_by_country", "country IS NULL")
        assert nulls == 0, f"rpt_revenue_by_country (Iceberg): {nulls} null country value(s)"

    def test_rpt_revenue_by_country_positive_revenue(self, ice_con):
        """rpt_revenue_by_country Iceberg table: total_revenue > 0 for all rows."""
        bad = self._count(
            ice_con, "rpt_revenue_by_country",
            "total_revenue IS NULL OR total_revenue <= 0"
        )
        assert bad == 0, f"rpt_revenue_by_country (Iceberg): {bad} row(s) with null/zero revenue"


# ---------------------------------------------------------------------------
# Stage 6 — Notebook execution
# ---------------------------------------------------------------------------

class TestNotebook:
    """Validate the Jupyter notebook executes cleanly end-to-end."""

    def test_notebook_exists(self):
        """notebook.ipynb must exist."""
        assert NOTEBOOK_PATH.exists(), f"Notebook not found: {NOTEBOOK_PATH}"

    def test_notebook_executes_cleanly(self, tmp_path):
        """nbconvert --execute must run the notebook without cell errors."""
        executed_nb = tmp_path / "notebook_executed.ipynb"
        result = _run(
            [
                _venv_python(), "-m", "jupyter", "nbconvert",
                "--to", "notebook",
                "--execute",
                "--ExecutePreprocessor.timeout=120",
                "--output", str(executed_nb),
                str(NOTEBOOK_PATH),
            ]
        )
        assert result.returncode == 0, (
            "notebook.ipynb failed during execution:\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )
        assert executed_nb.exists(), "Executed notebook output file was not created"
