#!/usr/bin/env python3
"""
extract.py — MSSQL → Arrow Parquet extraction layer.

Reads every user table from the configured MSSQL database and writes each
as a Parquet file under data/parquet/<table_name>.parquet, overwriting any
existing file (idempotent).

Usage
-----
    python extract.py                        # all tables
    python extract.py --tables orders items  # specific tables
    python extract.py --help

Connection
----------
Reads from environment variables (or a .env file if python-dotenv is
installed):

    MSSQL_SERVER   – hostname / IP  (default: localhost)
    MSSQL_PORT     – TCP port       (default: 1433)
    MSSQL_DATABASE – database name  (required)
    MSSQL_USER     – login          (required)
    MSSQL_PASSWORD – password       (required)
    MSSQL_DRIVER   – ODBC driver    (default: ODBC Driver 18 for SQL Server)
    MSSQL_TRUST_CERT – set to '1' to add TrustServerCertificate=yes
                       (useful for self-signed certs; default: 1)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import sqlalchemy as sa
from sqlalchemy import text

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL Server → Arrow type mapping
# ---------------------------------------------------------------------------

_MSSQL_TO_ARROW: dict[str, pa.DataType] = {
    # integers
    "bigint": pa.int64(),
    "int": pa.int32(),
    "integer": pa.int32(),
    "smallint": pa.int16(),
    "tinyint": pa.uint8(),
    # booleans
    "bit": pa.bool_(),
    # fixed-point / money
    "numeric": pa.float64(),
    "decimal": pa.float64(),
    "money": pa.float64(),
    "smallmoney": pa.float32(),
    # floating-point
    "float": pa.float64(),
    "real": pa.float32(),
    # date / time
    "date": pa.date32(),
    "time": pa.time64("us"),
    "datetime": pa.timestamp("us"),
    "datetime2": pa.timestamp("us"),
    "smalldatetime": pa.timestamp("s"),
    "datetimeoffset": pa.timestamp("us", tz="UTC"),
    # binary
    "binary": pa.binary(),
    "varbinary": pa.large_binary(),
    "image": pa.large_binary(),
    # strings (all map to large_utf8 to avoid 2 GB row-group limits)
    "char": pa.large_utf8(),
    "nchar": pa.large_utf8(),
    "varchar": pa.large_utf8(),
    "nvarchar": pa.large_utf8(),
    "text": pa.large_utf8(),
    "ntext": pa.large_utf8(),
    "xml": pa.large_utf8(),
    # misc
    "uniqueidentifier": pa.large_utf8(),
    "sql_variant": pa.large_utf8(),
}


def _sql_type_to_arrow(col_type: sa.types.TypeEngine) -> pa.DataType:
    """Return the best Arrow type for a SQLAlchemy column type."""
    type_name = type(col_type).__name__.lower()
    # SQLAlchemy names like "NVARCHAR", "INTEGER" → lower-case lookup
    arrow_type = _MSSQL_TO_ARROW.get(type_name)
    if arrow_type is not None:
        return arrow_type
    # Fallback: try the base class hierarchy
    for base in type(col_type).__mro__:
        arrow_type = _MSSQL_TO_ARROW.get(base.__name__.lower())
        if arrow_type is not None:
            return arrow_type
    log.debug("Unknown SQLAlchemy type %r — falling back to large_utf8", type_name)
    return pa.large_utf8()


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def _build_connection_url() -> str:
    """Build a SQLAlchemy connection URL from environment variables."""
    server = os.environ.get("MSSQL_SERVER", "localhost")
    port = os.environ.get("MSSQL_PORT", "1433")
    database = os.environ["MSSQL_DATABASE"]
    user = os.environ["MSSQL_USER"]
    password = os.environ["MSSQL_PASSWORD"]
    driver = os.environ.get("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")
    trust_cert = os.environ.get("MSSQL_TRUST_CERT", "1")

    query_parts = [f"driver={driver}"]
    if trust_cert == "1":
        query_parts.append("TrustServerCertificate=yes")

    query_string = ";".join(query_parts)

    # mssql+pyodbc://<user>:<password>@<server>:<port>/<database>?<odbc_connect>
    # We use the odbc_connect parameter to pass the full ODBC string so that
    # special characters in passwords are handled correctly.
    odbc_connect = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server},{port};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
    )
    if trust_cert == "1":
        odbc_connect += "TrustServerCertificate=yes;"

    import urllib.parse
    encoded = urllib.parse.quote_plus(odbc_connect)
    return f"mssql+pyodbc:///?odbc_connect={encoded}"


def _create_engine() -> sa.Engine:
    url = _build_connection_url()
    return sa.create_engine(url, pool_pre_ping=True)


# ---------------------------------------------------------------------------
# Table discovery
# ---------------------------------------------------------------------------

_TABLE_QUERY = text(
    """
    SELECT TABLE_SCHEMA, TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
      AND TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
    ORDER BY TABLE_SCHEMA, TABLE_NAME
    """
)


def _list_tables(conn: sa.Connection) -> list[tuple[str, str]]:
    """Return list of (schema, table_name) for all user base tables."""
    rows = conn.execute(_TABLE_QUERY).fetchall()
    return [(r[0], r[1]) for r in rows]


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def _extract_table(
    conn: sa.Connection,
    schema: str,
    table: str,
    out_dir: Path,
    chunk_size: int = 100_000,
) -> int:
    """
    Read *schema.table* in chunks and write to out_dir/<table>.parquet.
    Returns the total row count.
    """
    out_path = out_dir / f"{table}.parquet"

    # Reflect column metadata to build Arrow schema
    inspector = sa.inspect(conn)
    raw_columns = inspector.get_columns(table, schema=schema)
    arrow_fields = [
        pa.field(col["name"], _sql_type_to_arrow(col["type"]), nullable=True)
        for col in raw_columns
    ]
    arrow_schema = pa.schema(arrow_fields)

    log.info("  Extracting [%s].[%s] → %s", schema, table, out_path.name)

    select_sql = text(f"SELECT * FROM [{schema}].[{table}]")  # noqa: S608

    total_rows = 0
    writer: pq.ParquetWriter | None = None
    try:
        result = conn.execute(select_sql)
        col_names = list(result.keys())

        while True:
            rows = result.fetchmany(chunk_size)
            if not rows:
                break

            # Transpose rows → column-oriented dict
            columns: dict[str, list] = {name: [] for name in col_names}
            for row in rows:
                for name, value in zip(col_names, row):
                    columns[name].append(value)

            arrays = []
            for field in arrow_schema:
                col_data = columns[field.name]
                try:
                    arr = pa.array(col_data, type=field.type)
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                    # Graceful fallback: cast via Python repr → utf8
                    arr = pa.array(
                        [str(v) if v is not None else None for v in col_data],
                        type=pa.large_utf8(),
                    )
                arrays.append(arr)

            batch = pa.record_batch(arrays, schema=arrow_schema)

            if writer is None:
                writer = pq.ParquetWriter(out_path, arrow_schema, compression="snappy")
            writer.write_batch(batch)
            total_rows += len(rows)

        if writer is None:
            # Empty table — write an empty file so downstream tools see the schema
            empty_table = arrow_schema.empty_table()
            pq.write_table(empty_table, out_path, compression="snappy")

    finally:
        if writer is not None:
            writer.close()

    return total_rows


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _load_dotenv() -> None:
    """Load .env if python-dotenv is available (soft dependency)."""
    try:
        from dotenv import load_dotenv  # type: ignore[import]
        load_dotenv()
        log.debug(".env loaded via python-dotenv")
    except ImportError:
        pass


def main(argv: list[str] | None = None) -> int:
    _load_dotenv()

    parser = argparse.ArgumentParser(
        description="Extract MSSQL tables to Parquet files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        metavar="TABLE",
        help="Table names to extract (default: all user tables). "
             "Schema-qualified names like dbo.Orders are also accepted.",
    )
    parser.add_argument(
        "--out-dir",
        default="data/parquet",
        help="Output directory for Parquet files (default: data/parquet).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100_000,
        help="Rows per read chunk (default: 100000).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args(argv)

    logging.getLogger().setLevel(args.log_level)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Validate required env vars
    missing = [v for v in ("MSSQL_DATABASE", "MSSQL_USER", "MSSQL_PASSWORD") if not os.environ.get(v)]
    if missing:
        log.error("Missing required environment variables: %s", ", ".join(missing))
        log.error("Set them directly or add them to a .env file.")
        return 1

    wall_start = time.perf_counter()

    try:
        engine = _create_engine()
        with engine.connect() as conn:
            # Resolve table list
            if args.tables:
                tables: list[tuple[str, str]] = []
                all_tables = {name: schema for schema, name in _list_tables(conn)}
                for spec in args.tables:
                    if "." in spec:
                        schema, name = spec.split(".", 1)
                    else:
                        schema = all_tables.get(spec, "dbo")
                        name = spec
                    tables.append((schema, name))
            else:
                tables = _list_tables(conn)

            if not tables:
                log.warning("No tables found — nothing to extract.")
                return 0

            log.info("Extracting %d table(s) to %s/", len(tables), out_dir)

            results: list[tuple[str, str, int, float]] = []
            for schema, table in tables:
                t0 = time.perf_counter()
                try:
                    rows = _extract_table(conn, schema, table, out_dir, args.chunk_size)
                    elapsed = time.perf_counter() - t0
                    results.append((schema, table, rows, elapsed))
                    log.info(
                        "  ✓ [%s].[%s]: %s rows in %.2fs",
                        schema, table, f"{rows:,}", elapsed,
                    )
                except Exception:
                    log.exception("  ✗ Failed to extract [%s].[%s]", schema, table)
                    results.append((schema, table, -1, time.perf_counter() - t0))

    except sa.exc.OperationalError as exc:
        log.error("Cannot connect to SQL Server: %s", exc)
        return 1

    wall_elapsed = time.perf_counter() - wall_start
    successes = [r for r in results if r[2] >= 0]
    failures = [r for r in results if r[2] < 0]

    log.info("")
    log.info("── Summary ──────────────────────────────────")
    log.info("  Tables extracted : %d / %d", len(successes), len(results))
    log.info("  Total rows       : %s", f"{sum(r[2] for r in successes):,}")
    log.info("  Wall time        : %.2fs", wall_elapsed)
    if failures:
        log.warning("  Failed tables    : %s", ", ".join(f"[{r[0]}].[{r[1]}]" for r in failures))
    log.info("─────────────────────────────────────────────")

    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
