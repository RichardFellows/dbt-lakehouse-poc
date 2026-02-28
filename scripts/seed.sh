#!/usr/bin/env bash
# seed.sh: Wait for MSSQL to be healthy, then run init-db.sql
set -euo pipefail

CONTAINER="${MSSQL_CONTAINER:-lakehouse-mssql}"
SA_PASSWORD="${SA_PASSWORD:-YourStrong!Passw0rd}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="${SCRIPT_DIR}/init-db.sql"
MAX_WAIT=120  # seconds

echo "==> Waiting for MSSQL container '${CONTAINER}' to be healthy..."

elapsed=0
while true; do
    status=$(docker inspect --format='{{.State.Health.Status}}' "${CONTAINER}" 2>/dev/null || echo "not_found")
    if [ "${status}" = "healthy" ]; then
        echo "    Container is healthy."
        break
    fi
    if [ "${status}" = "not_found" ]; then
        echo "ERROR: Container '${CONTAINER}' not found. Run 'make docker-up' first." >&2
        exit 1
    fi
    if [ "${elapsed}" -ge "${MAX_WAIT}" ]; then
        echo "ERROR: Timed out waiting for MSSQL to become healthy (${MAX_WAIT}s)." >&2
        exit 1
    fi
    echo "    Status: ${status} — waiting... (${elapsed}s elapsed)"
    sleep 5
    elapsed=$((elapsed + 5))
done

echo "==> Running init-db.sql against ${CONTAINER}..."

docker exec -i "${CONTAINER}" \
    /opt/mssql-tools18/bin/sqlcmd \
    -S localhost \
    -U SA \
    -P "${SA_PASSWORD}" \
    -No \
    -i /scripts/init-db.sql

echo "==> Seed complete."
