FROM python:3.11-slim

# System dependencies for pyodbc + MSSQL ODBC driver
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    unixodbc-dev \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"

# Install PowerShell
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/microsoft-prod.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends powershell \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency definition first for layer caching
COPY pyproject.toml ./

# Create venv and install dependencies
RUN uv venv .venv && uv pip install --python .venv/bin/python -e ".[dev]" || true

# Copy the rest of the project
COPY . .

# Reinstall with full project context (needed for editable install)
RUN uv pip install --python .venv/bin/python -e ".[dev]"

ENV PATH="/app/.venv/bin:$PATH"
ENV MSSQL_DRIVER="ODBC Driver 18 for SQL Server"
ENV MSSQL_TRUST_CERT=1
