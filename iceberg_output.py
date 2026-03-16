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
DEFAULT_DB = PROJECT_ROOT / "output" / "analytics.duckdb"
DEFAULT_WAREHOUSE = "warehouse"  # Must match nessie.catalog.default-warehouse
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
    PyIceberg's ``RestCatalog`` at that endpoint.

    The ``warehouse`` parameter sent to Nessie must match one of its
    configured warehouse names (e.g. ``warehouse``).  Nessie pushes
    object-store config (S3 credentials, region, endpoint) back to
    PyIceberg via the REST config response — no client-side S3 config
    needed.

    For local dev (Docker Compose), Nessie + LocalStack run together and
    ``warehouse`` defaults to the Nessie-configured name.
    """
    catalog_props = {
        "uri": f"{nessie_url.rstrip('/')}/iceberg",
        # Must match a warehouse name configured in Nessie server.
        "warehouse": warehouse,
        # Nessie maps each Iceberg REST "prefix" to a branch;
        # "main" is the default branch name.
        "prefix": "main",
    }

    # Allow overriding S3 endpoint for host-side access to LocalStack.
    # Nessie pushes the Docker-internal hostname (e.g. http://localstack:4566)
    # which isn't reachable from the host. Set S3_ENDPOINT=http://localhost:4566
    # when running outside Docker.
    s3_endpoint = os.environ.get("S3_ENDPOINT")
    if s3_endpoint:
        catalog_props["s3.endpoint"] = s3_endpoint
        catalog_props["s3.access-key-id"] = os.environ.get("AWS_ACCESS_KEY_ID", "test")
        catalog_props["s3.secret-access-key"] = os.environ.get("AWS_SECRET_ACCESS_KEY", "test")
        catalog_props["s3.region"] = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

    catalog = RestCatalog("lakehouse", **catalog_props)

    # Post-init override: Nessie server pushes Docker-internal S3 endpoint
    # (e.g. http://localstack:4566) via REST config overrides, which takes
    # precedence over client-side properties. When running from the host,
    # we must force the endpoint back to the host-accessible URL.
    if s3_endpoint:
        catalog.properties["s3.endpoint"] = s3_endpoint
        catalog.properties["s3.access-key-id"] = os.environ.get("AWS_ACCESS_KEY_ID", "test")
        catalog.properties["s3.secret-access-key"] = os.environ.get("AWS_SECRET_ACCESS_KEY", "test")
        catalog.properties["s3.region"] = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

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


def _apply_s3_overrides(table) -> None:
    """Override S3 endpoint on a table's IO if S3_ENDPOINT is set.

    Nessie pushes the Docker-internal S3 endpoint (e.g. http://localstack:4566)
    to PyIceberg via the REST catalog config. When running from the host,
    we must patch the table's IO properties to use the host-accessible endpoint.
    """
    s3_endpoint = os.environ.get("S3_ENDPOINT")
    if not s3_endpoint:
        return

    overrides = {
        "s3.endpoint": s3_endpoint,
        "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        "s3.region": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    }

    # Patch the table's IO properties and reinitialise the IO
    table.metadata_location  # ensure metadata loaded
    io_props = dict(table.io.properties) if hasattr(table.io, "properties") else {}
    io_props.update(overrides)

    from pyiceberg.io import load_file_io

    table._io = load_file_io(io_props, table.metadata.location)


def write_iceberg_table(
    catalog: RestCatalog,
    table_name: str,
    arrow_table: pa.Table,
) -> None:
    """Write a PyArrow table as an Iceberg table, overwriting if it exists.

    The table location is derived by Nessie from the warehouse config —
    no client-side location override needed.  Data files land wherever
    the warehouse is configured to point (S3/LocalStack in CI, or S3/LocalStack
    locally via Docker Compose).
    """
    identifier = f"{DEFAULT_NAMESPACE}.{table_name}"

    try:
        catalog.drop_table(identifier)
        logger.debug("Dropped existing table '%s'", identifier)
    except NoSuchTableError:
        pass

    iceberg_table = catalog.create_table(
        identifier,
        schema=arrow_table.schema,
    )
    _apply_s3_overrides(iceberg_table)
    iceberg_table.overwrite(arrow_table)
    logger.debug("Wrote %d rows to '%s'", len(arrow_table), identifier)


def verify_iceberg_table(
    catalog: RestCatalog,
    table_name: str,
) -> int:
    """Read an Iceberg table back via PyIceberg and return row count.

    Uses the catalog to load the table and scan it — works regardless
    of whether the data lives on local filesystem or S3/LocalStack.
    """
    identifier = f"{DEFAULT_NAMESPACE}.{table_name}"
    table = catalog.load_table(identifier)
    _apply_s3_overrides(table)
    # Use to_arrow() to read the full table via the catalog's FileIO
    scan = table.scan()
    arrow_table = scan.to_arrow()
    return len(arrow_table)


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
        default="warehouse",
        help="Nessie warehouse name (must match server config, default: warehouse)",
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

        # 3. Verify round-trip via catalog scan
        verified_count = verify_iceberg_table(catalog, table_name)
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
