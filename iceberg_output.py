"""Export dbt output tables from DuckDB to Apache Iceberg format.

Reads materialized tables from the dbt DuckDB database and writes each
as an Iceberg table via a **Nessie REST catalog** running in Docker.
A verification step reads every table back via DuckDB's ``iceberg_scan``
to confirm round-trip fidelity.

Nessie exposes a standard Iceberg REST catalog endpoint at:
    http://localhost:19120/iceberg

Start the catalog with ``make docker-up`` (or ``docker compose up -d nessie``)
before running this script.

Usage:
    python iceberg_output.py
    python iceberg_output.py --db output/analytics.duckdb
    python iceberg_output.py --nessie-url http://localhost:19120
    python iceberg_output.py --tables orders_enriched rpt_customer_summary
    python iceberg_output.py --log-level DEBUG
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from pathlib import Path

import duckdb
import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchTableError,
)

logger = logging.getLogger(__name__)

# ── defaults ────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DB = PROJECT_ROOT / "dbt_project" / "target" / "lakehouse.duckdb"
DEFAULT_WAREHOUSE = str(PROJECT_ROOT / "output" / "iceberg" / "warehouse")
DEFAULT_NESSIE_URL = os.environ.get("NESSIE_URL", "http://localhost:19120")
DEFAULT_NAMESPACE = "default"

TABLES = [
    "orders_enriched",
    "rpt_revenue_by_country",
    "rpt_revenue_by_category",
    "rpt_customer_summary",
    "rpt_product_performance",
]


# ── helpers ─────────────────────────────────────────────────────────
def setup_catalog(nessie_url: str, warehouse: str) -> RestCatalog:
    """Connect to the Nessie REST catalog.

    Nessie exposes the Iceberg REST spec at ``<url>/iceberg``.  We point
    PyIceberg's ``RestCatalog`` at that endpoint and use the local
    filesystem as the warehouse (where data files are written).
    """
    Path(warehouse).mkdir(parents=True, exist_ok=True)

    catalog = RestCatalog(
        "lakehouse",
        **{
            "uri": f"{nessie_url.rstrip('/')}/iceberg",
            "warehouse": warehouse,
            # Nessie maps each Iceberg REST "prefix" to a branch;
            # "main" is the default branch name.
            "prefix": "main",
        },
    )

    try:
        catalog.create_namespace(DEFAULT_NAMESPACE)
        logger.info("Created namespace '%s'", DEFAULT_NAMESPACE)
    except NamespaceAlreadyExistsError:
        logger.debug("Namespace '%s' already exists", DEFAULT_NAMESPACE)

    return catalog


def read_duckdb_table(db_path: Path, table_name: str) -> pa.Table:
    """Read a DuckDB table and return it as a PyArrow Table."""
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        arrow_table = con.execute(f"SELECT * FROM {table_name}").fetch_arrow_table()
        return arrow_table
    finally:
        con.close()


def write_iceberg_table(
    catalog: RestCatalog,
    table_name: str,
    arrow_table: pa.Table,
) -> None:
    """Write a PyArrow table as an Iceberg table, overwriting if it exists."""
    identifier = f"{DEFAULT_NAMESPACE}.{table_name}"

    try:
        catalog.drop_table(identifier)
        logger.debug("Dropped existing table '%s'", identifier)
    except NoSuchTableError:
        pass

    iceberg_table = catalog.create_table(identifier, schema=arrow_table.schema)
    iceberg_table.overwrite(arrow_table)
    logger.debug("Wrote %d rows to '%s'", len(arrow_table), identifier)


def verify_iceberg_table(
    warehouse: str,
    table_name: str,
) -> int:
    """Read an Iceberg table back via DuckDB iceberg_scan and return row count."""
    metadata_path = (
        Path(warehouse)
        / DEFAULT_NAMESPACE
        / table_name
        / "metadata"
    )
    # Find the latest metadata JSON file
    metadata_files = sorted(metadata_path.glob("*.metadata.json"))
    if not metadata_files:
        raise FileNotFoundError(
            f"No metadata file found for table '{table_name}' in {metadata_path}"
        )
    latest_metadata = metadata_files[-1]

    con = duckdb.connect()
    try:
        con.install_extension("iceberg")
        con.load_extension("iceberg")
        result = con.execute(
            f"SELECT count(*) FROM iceberg_scan('{latest_metadata}')"
        ).fetchone()
        return result[0]
    finally:
        con.close()


# ── main ────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export dbt DuckDB tables to Iceberg format via Nessie",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help=f"Path to DuckDB database (default: {DEFAULT_DB})",
    )
    parser.add_argument(
        "--nessie-url",
        default=DEFAULT_NESSIE_URL,
        help=f"Nessie server base URL (default: {DEFAULT_NESSIE_URL})",
    )
    parser.add_argument(
        "--warehouse",
        default=DEFAULT_WAREHOUSE,
        help=f"Local filesystem warehouse path for data files (default: {DEFAULT_WAREHOUSE})",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=TABLES,
        help="Tables to export (default: all five dbt output tables)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    db_path: Path = args.db.resolve()

    if not db_path.exists():
        logger.error("DuckDB database not found: %s", db_path)
        raise SystemExit(1)

    logger.info("DuckDB source  : %s", db_path)
    logger.info("Nessie catalog : %s/iceberg  (branch: main)", args.nessie_url)
    logger.info("Warehouse      : %s", args.warehouse)
    logger.info("Tables         : %s", ", ".join(args.tables))

    catalog = setup_catalog(args.nessie_url, args.warehouse)

    total_start = time.perf_counter()

    for table_name in args.tables:
        t0 = time.perf_counter()

        # 1. Read from DuckDB
        logger.info("Reading '%s' from DuckDB …", table_name)
        arrow_table = read_duckdb_table(db_path, table_name)
        row_count = len(arrow_table)

        # 2. Write to Iceberg via Nessie
        logger.info("Writing '%s' to Iceberg (%d rows) …", table_name, row_count)
        write_iceberg_table(catalog, table_name, arrow_table)

        # 3. Verify round-trip via DuckDB iceberg_scan
        verified_count = verify_iceberg_table(args.warehouse, table_name)
        elapsed = time.perf_counter() - t0

        if verified_count != row_count:
            logger.error(
                "MISMATCH on '%s': wrote %d rows, read back %d",
                table_name,
                row_count,
                verified_count,
            )
        else:
            logger.info(
                "✓ %-30s  %5d rows  %.2fs",
                table_name,
                row_count,
                elapsed,
            )

    total_elapsed = time.perf_counter() - total_start
    logger.info("Done — %d tables exported in %.2fs", len(args.tables), total_elapsed)


if __name__ == "__main__":
    main()
