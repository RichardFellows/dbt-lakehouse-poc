<#
.SYNOPSIS
    Cross-platform build script for the dbt Lakehouse POC.
.DESCRIPTION
    Usage: pwsh run.ps1 <target>
    Targets: setup, docker-up, nessie-wait, ensure-bucket, seed, extract, transform,
             test, load-iceberg, notebook, all, ci, ci-full, test-e2e, clean, docker-down
.EXAMPLE
    pwsh run.ps1 all         # Full pipeline
    pwsh run.ps1 transform   # Just run dbt
    pwsh run.ps1 notebook    # Launch Jupyter
#>

param(
    [Parameter(Position=0, Mandatory=$true)]
    [ValidateSet("setup","docker-up","nessie-wait","ensure-bucket","seed","extract",
                 "transform","test","load-iceberg","notebook","all","ci","ci-full",
                 "test-e2e","clean","docker-down")]
    [string]$Target
)

$ErrorActionPreference = "Stop"

# ── Cross-platform paths ────────────────────────────────────────────────────
$VENV = ".venv"
if ($IsWindows -or ($env:OS -eq "Windows_NT")) {
    $PYTHON = Join-Path $VENV "Scripts/python.exe"
    $DBT = Join-Path $VENV "Scripts/dbt.exe"
    $PYTEST = Join-Path $VENV "Scripts/pytest.exe"
    $AWSLOCAL = Join-Path $VENV "Scripts/awslocal.exe"
} else {
    $PYTHON = Join-Path $VENV "bin/python"
    $DBT = Join-Path $VENV "bin/dbt"
    $PYTEST = Join-Path $VENV "bin/pytest"
    $AWSLOCAL = Join-Path $VENV "bin/awslocal"
}
$DBT_DIR = "dbt_project"

# ── Helper: load .env file into environment ──────────────────────────────────
function Load-DotEnv {
    if (-not (Test-Path ".env")) {
        Write-Error "ERROR: .env not found. Copy .env.example to .env and fill in values."
        exit 1
    }
    Get-Content ".env" | ForEach-Object {
        if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
            [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim(), "Process")
        }
    }
}

# ── Targets ──────────────────────────────────────────────────────────────────

function Invoke-Setup {
    Write-Host "Creating virtual environment..."
    uv venv $VENV
    uv pip install --python $PYTHON -e ".[dev]"
    Write-Host "✓ Environment ready (uv + pyproject.toml)" -ForegroundColor Green
}

function Invoke-DockerUp {
    Load-DotEnv
    docker compose up -d
    Write-Host "✓ Containers started." -ForegroundColor Green
    docker compose ps
}

function Invoke-NessieWait {
    $nessieUrl = if ($env:NESSIE_URL) { $env:NESSIE_URL } else { "http://localhost:19120" }
    Write-Host "Waiting for Nessie catalog at $nessieUrl ..."
    for ($i = 1; $i -le 30; $i++) {
        try {
            $null = Invoke-RestMethod "$nessieUrl/api/v2/config" -TimeoutSec 3
            Write-Host "✓ Nessie is ready." -ForegroundColor Green
            return
        } catch {
            Write-Host "  attempt $i/30 - retrying in 2s ..."
            Start-Sleep -Seconds 2
        }
    }
    Write-Error "ERROR: Nessie did not become ready in time."
    exit 1
}

function Invoke-EnsureBucket {
    $inDocker = Test-Path "/.dockerenv"
    $s3Endpoint = if ($env:S3_ENDPOINT) { $env:S3_ENDPOINT }
                  elseif ($inDocker) { "http://localstack:4566" }
                  else { "http://localhost:4566" }
    Write-Host "Ensuring S3 bucket 'lakehouse' exists on LocalStack ($s3Endpoint)..."

    # Wait for LocalStack to be reachable
    for ($i = 1; $i -le 20; $i++) {
        try {
            $null = Invoke-RestMethod "$s3Endpoint/_localstack/health" -TimeoutSec 3
            break
        } catch {
            Write-Host "  attempt $i/20 - waiting for LocalStack..."
            Start-Sleep -Seconds 2
        }
    }

    # Create bucket
    $env:AWS_DEFAULT_REGION = if ($env:AWS_REGION) { $env:AWS_REGION } else { "us-east-1" }
    if (-not $env:AWS_ACCESS_KEY_ID) { $env:AWS_ACCESS_KEY_ID = "test" }
    if (-not $env:AWS_SECRET_ACCESS_KEY) { $env:AWS_SECRET_ACCESS_KEY = "test" }

    try {
        & $AWSLOCAL --endpoint-url $s3Endpoint s3 mb s3://lakehouse 2>$null
        Write-Host "✓ S3 bucket 'lakehouse' created." -ForegroundColor Green
    } catch {
        $buckets = & $AWSLOCAL --endpoint-url $s3Endpoint s3 ls 2>$null
        if ($buckets -match "lakehouse") {
            Write-Host "✓ S3 bucket 'lakehouse' already exists." -ForegroundColor Green
        } else {
            Write-Error "Failed to create S3 bucket: $_"
        }
    }
}

function Invoke-Seed {
    Load-DotEnv
    $sa_password = $env:SA_PASSWORD
    $mssqlServer = if ($env:MSSQL_SERVER) { $env:MSSQL_SERVER } else { "localhost" }
    $mssqlPort = if ($env:MSSQL_PORT) { $env:MSSQL_PORT } else { "1433" }
    $inDocker = Test-Path "/.dockerenv"

    if ($inDocker) {
        # Running inside Docker — use sqlcmd directly against the MSSQL service
        Write-Host "Waiting for MSSQL at ${mssqlServer}:${mssqlPort}..."
        for ($i = 1; $i -le 30; $i++) {
            try {
                $result = & /opt/mssql-tools18/bin/sqlcmd -S "$mssqlServer,$mssqlPort" -U SA -P $sa_password -No -Q "SELECT 1" 2>&1
                if ($LASTEXITCODE -eq 0) {
                    Write-Host "  MSSQL is ready."
                    break
                }
            } catch {}
            Write-Host "  attempt $i/30 - retrying in 2s..."
            Start-Sleep -Seconds 2
        }
        Write-Host "Seeding database..."
        & /opt/mssql-tools18/bin/sqlcmd -S "$mssqlServer,$mssqlPort" -U SA -P $sa_password -No -i scripts/init-db.sql
    } else {
        # Running on host — use docker exec
        $container = "lakehouse-mssql"
        Write-Host "Waiting for MSSQL to be healthy..."
        for ($i = 1; $i -le 30; $i++) {
            $health = docker inspect --format='{{.State.Health.Status}}' $container 2>$null
            if ($health -eq "healthy") {
                Write-Host "  MSSQL is healthy."
                break
            }
            Write-Host "  attempt $i/30 - waiting for MSSQL ($health)..."
            Start-Sleep -Seconds 2
        }
        Write-Host "Seeding database..."
        docker exec $container /opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P $sa_password -No -i /scripts/init-db.sql
    }
    Write-Host "✓ Database seeded." -ForegroundColor Green
}

function Invoke-Extract {
    Load-DotEnv
    & $PYTHON extract.py
    if ($LASTEXITCODE -ne 0) { throw "extract.py failed" }
    Write-Host "✓ Parquet files written to data/parquet/" -ForegroundColor Green
}

function Invoke-Transform {
    if (-not (Test-Path "output")) { New-Item -ItemType Directory "output" | Out-Null }
    if (-not (Test-Path "data/parquet")) { New-Item -ItemType Directory "data/parquet" -Force | Out-Null }
    Push-Location $DBT_DIR
    try {
        & "../$DBT" run --profiles-dir .
        if ($LASTEXITCODE -ne 0) { throw "dbt run failed" }
        Write-Host "✓ dbt models materialised." -ForegroundColor Green
    } finally {
        Pop-Location
    }
}

function Invoke-Test {
    Push-Location $DBT_DIR
    try {
        & "../$DBT" test --profiles-dir .
        if ($LASTEXITCODE -ne 0) { throw "dbt test failed" }
        Write-Host "✓ dbt tests passed." -ForegroundColor Green
    } finally {
        Pop-Location
    }
}

function Invoke-LoadIceberg {
    Invoke-NessieWait
    & $PYTHON iceberg_output.py
    if ($LASTEXITCODE -ne 0) { throw "iceberg_output.py failed" }
    Write-Host "✓ Iceberg tables written (catalogued in Nessie)" -ForegroundColor Green
}

function Invoke-Notebook {
    & $PYTHON -m jupyter notebook notebook.ipynb
}

function Invoke-All {
    $inDocker = Test-Path "/.dockerenv"
    if (-not $inDocker) {
        Invoke-Setup
        Invoke-DockerUp
    }
    Invoke-NessieWait
    Invoke-EnsureBucket
    Invoke-Seed
    Invoke-Extract
    Invoke-Transform
    Invoke-LoadIceberg
    Write-Host "`n✓ Full pipeline complete." -ForegroundColor Green
    if (-not $inDocker) {
        Write-Host "Run 'pwsh run.ps1 notebook' to explore results."
    }
}

function Invoke-CI {
    Invoke-Extract
    Invoke-Transform
    Invoke-Test
    Write-Host "✓ CI pipeline passed." -ForegroundColor Green
}

function Invoke-TestE2E {
    & $PYTEST tests/test_e2e.py -v --tb=short
    if ($LASTEXITCODE -ne 0) { throw "e2e tests failed" }
    Write-Host "✓ E2E test suite passed." -ForegroundColor Green
}

function Invoke-CIFull {
    Invoke-Extract
    Invoke-Transform
    Invoke-Test
    Invoke-LoadIceberg
    Invoke-TestE2E
    Write-Host "✓ Full CI pipeline (including e2e) passed." -ForegroundColor Green
}

function Invoke-Clean {
    if (Test-Path $VENV) { Remove-Item -Recurse -Force $VENV }
    if (Test-Path "output") { Remove-Item -Recurse -Force "output" }
    Get-ChildItem -Recurse -Directory -Filter "__pycache__" -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force
    Get-ChildItem -Recurse -Filter "*.pyc" -ErrorAction SilentlyContinue | Remove-Item -Force
    Get-ChildItem -Recurse -Directory -Filter ".ipynb_checkpoints" -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force
    Write-Host "✓ Clean complete." -ForegroundColor Green
}

function Invoke-DockerDown {
    docker compose down
    Write-Host "✓ Containers stopped." -ForegroundColor Green
}

# ── Dispatch ─────────────────────────────────────────────────────────────────
switch ($Target) {
    "setup"          { Invoke-Setup }
    "docker-up"      { Invoke-DockerUp }
    "nessie-wait"    { Invoke-NessieWait }
    "ensure-bucket"  { Invoke-EnsureBucket }
    "seed"           { Invoke-Seed }
    "extract"        { Invoke-Extract }
    "transform"      { Invoke-Transform }
    "test"           { Invoke-Test }
    "load-iceberg"   { Invoke-LoadIceberg }
    "notebook"       { Invoke-Notebook }
    "all"            { Invoke-All }
    "ci"             { Invoke-CI }
    "ci-full"        { Invoke-CIFull }
    "test-e2e"       { Invoke-TestE2E }
    "clean"          { Invoke-Clean }
    "docker-down"    { Invoke-DockerDown }
}
