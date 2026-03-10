.PHONY: setup docker-up nessie-wait seed extract transform load-iceberg notebook all ci test test-e2e ci-full clean docker-down

VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
DBT := cd dbt_project && PATH=../$(VENV)/bin:$$PATH

## setup: create virtual environment and install all dependencies
setup:
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@echo "✓ Environment ready. Activate with: source $(VENV)/bin/activate"

## docker-up: start MSSQL and Nessie containers (requires .env)
docker-up:
	@if [ ! -f .env ]; then \
		echo "ERROR: .env not found. Copy .env.example to .env and fill in values."; \
		exit 1; \
	fi
	docker compose up -d
	@echo "✓ MSSQL + Nessie containers started. Waiting for healthchecks..."
	docker compose ps

## nessie-wait: block until the Nessie catalog API is reachable
## Respects NESSIE_URL env var (default: http://localhost:19120)
NESSIE_BASE_URL ?= $(if $(NESSIE_URL),$(NESSIE_URL),http://localhost:19120)
nessie-wait:
	@echo "Waiting for Nessie catalog at $(NESSIE_BASE_URL) …"
	@for i in $$(seq 1 30); do \
		curl -sf $(NESSIE_BASE_URL)/api/v2/config > /dev/null 2>&1 && echo "✓ Nessie is ready." && exit 0; \
		echo "  attempt $$i/30 — retrying in 2s …"; \
		sleep 2; \
	done; \
	echo "ERROR: Nessie did not become ready in time."; exit 1

## seed: wait for MSSQL to be healthy, then load init-db.sql
seed:
	@if [ ! -f .env ]; then \
		echo "ERROR: .env not found. Copy .env.example to .env and fill in values."; \
		exit 1; \
	fi
	SA_PASSWORD=$$(grep '^SA_PASSWORD=' .env | cut -d= -f2-) \
		bash scripts/seed.sh
	@echo "✓ Database seeded."

## extract: pull all tables from MSSQL and write to data/parquet/ (requires .env)
extract:
	@if [ ! -f .env ]; then \
		echo "ERROR: .env not found. Copy .env.example to .env and fill in values."; \
		exit 1; \
	fi
	$(PYTHON) extract.py
	@echo "✓ Parquet files written to data/parquet/"

## transform: run dbt models
transform:
	@mkdir -p output data/parquet
	$(DBT) dbt run --profiles-dir .
	@echo "✓ dbt models materialised."

## test: run dbt tests
test:
	$(DBT) dbt test --profiles-dir .
	@echo "✓ dbt tests passed."

## load-iceberg: export dbt output tables to Iceberg via Nessie REST catalog
load-iceberg: nessie-wait
	$(PYTHON) iceberg_output.py
	@echo "✓ Iceberg tables written to output/iceberg/ (catalogued in Nessie)"

## notebook: launch Jupyter notebook for Iceberg analytics
notebook:
	$(PYTHON) -m jupyter notebook notebook.ipynb

## all: run the full pipeline end-to-end
all: setup docker-up nessie-wait seed extract transform load-iceberg
	@echo "✓ Full pipeline complete. Run 'make notebook' to explore results."

## ci: CI-friendly pipeline (assumes MSSQL already running and seeded)
ci: extract transform test
	@echo "✓ CI pipeline passed."

## test-e2e: run the full end-to-end pytest suite (assumes Docker services running and DB seeded)
test-e2e:
	$(VENV)/bin/pytest tests/test_e2e.py -v --tb=short
	@echo "✓ E2E test suite passed."

## ci-full: full CI pipeline including end-to-end tests (assumes Docker services running and DB seeded)
ci-full: extract transform test load-iceberg test-e2e
	@echo "✓ Full CI pipeline (including e2e) passed."

## clean: remove virtual environment, output files, and caches
clean:
	rm -rf $(VENV)
	rm -rf output/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".ipynb_checkpoints" -exec rm -rf {} + 2>/dev/null || true
	@echo "✓ Clean complete."

## docker-down: stop and remove containers
docker-down:
	docker compose down
	@echo "✓ Containers stopped."
