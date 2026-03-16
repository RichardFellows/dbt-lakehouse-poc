<#
.SYNOPSIS
    PowerShell equivalent of the Makefile for Windows environments.
.DESCRIPTION
    Usage: .\run.ps1 <target>
    Targets: setup, docker-up, nessie-wait, seed, extract, transform, test,
             load-iceberg, notebook, all, ci, ci-full, test-e2e, clean, docker-down
.EXAMPLE
    .\run.ps1 all         # Full pipeline
    .\run.ps1 transform   # Just run dbt
    .\run.ps1 notebook    # Launch Jupyter
#>

param(
    [Parameter(Position=0, Mandatory=$true)]
    [ValidateSet("setup","docker-up","nessie-wait","seed","extract","transform",
                 "test","load-iceberg","notebook","all","ci","ci-full","test-e2e",
                 "clean","docker-down")]
    [string]$Target
)

$ErrorActionPreference = "Stop"

$VENV = ".venv"
$PYTHON = "$VENV\Scripts\python.exe"
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
    uv pip install --python $PYTHON -e ".[test]"
    Write-Host "✓ Environment ready. Activate with: .venv\Scripts\activate" -ForegroundColor Green
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

function Invoke-Seed {
    Load-DotEnv
    $sa_password = $env:SA_PASSWORD
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
    Write-Host "✓ Database seeded." -ForegroundColor Green
}

function Invoke-Extract {
    Load-DotEnv
    & $PYTHON extract.py
    Write-Host "✓ Parquet files written to data/parquet/" -ForegroundColor Green
}

function Invoke-Transform {
    if (-not (Test-Path "output")) { New-Item -ItemType Directory "output" | Out-Null }
    if (-not (Test-Path "data\parquet")) { New-Item -ItemType Directory "data\parquet" -Force | Out-Null }
    Push-Location $DBT_DIR
    try {
        & "..\$VENV\Scripts\dbt.exe" run --profiles-dir .
        Write-Host "✓ dbt models materialised." -ForegroundColor Green
    } finally {
        Pop-Location
    }
}

function Invoke-Test {
    Push-Location $DBT_DIR
    try {
        & "..\$VENV\Scripts\dbt.exe" test --profiles-dir .
        Write-Host "✓ dbt tests passed." -ForegroundColor Green
    } finally {
        Pop-Location
    }
}

function Invoke-LoadIceberg {
    Invoke-NessieWait
    & $PYTHON iceberg_output.py
    Write-Host "✓ Iceberg tables written (catalogued in Nessie)" -ForegroundColor Green
}

function Invoke-Notebook {
    & $PYTHON -m jupyter notebook notebook.ipynb
}

function Invoke-All {
    Invoke-Setup
    Invoke-DockerUp
    Invoke-NessieWait
    Invoke-Seed
    Invoke-Extract
    Invoke-Transform
    Invoke-LoadIceberg
    Write-Host "`n✓ Full pipeline complete. Run '.\run.ps1 notebook' to explore results." -ForegroundColor Green
}

function Invoke-CI {
    Invoke-Extract
    Invoke-Transform
    Invoke-Test
    Write-Host "✓ CI pipeline passed." -ForegroundColor Green
}

function Invoke-TestE2E {
    & "$VENV\Scripts\pytest.exe" tests/test_e2e.py -v --tb=short
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
    Get-ChildItem -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force
    Get-ChildItem -Recurse -Filter "*.pyc" | Remove-Item -Force
    Get-ChildItem -Recurse -Directory -Filter ".ipynb_checkpoints" | Remove-Item -Recurse -Force
    Write-Host "✓ Clean complete." -ForegroundColor Green
}

function Invoke-DockerDown {
    docker compose down
    Write-Host "✓ Containers stopped." -ForegroundColor Green
}

# ── Dispatch ─────────────────────────────────────────────────────────────────
switch ($Target) {
    "setup"         { Invoke-Setup }
    "docker-up"     { Invoke-DockerUp }
    "nessie-wait"   { Invoke-NessieWait }
    "seed"          { Invoke-Seed }
    "extract"       { Invoke-Extract }
    "transform"     { Invoke-Transform }
    "test"          { Invoke-Test }
    "load-iceberg"  { Invoke-LoadIceberg }
    "notebook"      { Invoke-Notebook }
    "all"           { Invoke-All }
    "ci"            { Invoke-CI }
    "ci-full"       { Invoke-CIFull }
    "test-e2e"      { Invoke-TestE2E }
    "clean"         { Invoke-Clean }
    "docker-down"   { Invoke-DockerDown }
}
