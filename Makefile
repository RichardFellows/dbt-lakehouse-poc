.PHONY: setup extract docker-up docker-down seed clean

VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

## setup: create virtual environment and install all dependencies
setup:
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@echo "✓ Environment ready. Activate with: source $(VENV)/bin/activate"

## extract: pull all tables from MSSQL and write to data/parquet/ (requires .env)
extract:
	@if [ ! -f .env ]; then \
		echo "ERROR: .env not found. Copy .env.example to .env and fill in values."; \
		exit 1; \
	fi
	$(PYTHON) extract.py
	@echo "✓ Parquet files written to data/parquet/"

## docker-up: start MSSQL container (requires .env)
docker-up:
	@if [ ! -f .env ]; then \
		echo "ERROR: .env not found. Copy .env.example to .env and fill in values."; \
		exit 1; \
	fi
	docker compose up -d
	@echo "✓ MSSQL container started. Waiting for healthcheck..."
	docker compose ps

## seed: wait for MSSQL to be healthy, then load init-db.sql
seed:
	@if [ ! -f .env ]; then \
		echo "ERROR: .env not found. Copy .env.example to .env and fill in values."; \
		exit 1; \
	fi
	SA_PASSWORD=$$(grep '^SA_PASSWORD=' .env | cut -d= -f2-) \
		bash scripts/seed.sh
	@echo "✓ Database seeded."

## docker-down: stop and remove containers
docker-down:
	docker compose down
	@echo "✓ Containers stopped."

## clean: remove virtual environment, output files, and caches
clean:
	rm -rf $(VENV)
	rm -rf output/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".ipynb_checkpoints" -exec rm -rf {} + 2>/dev/null || true
	@echo "✓ Clean complete."
