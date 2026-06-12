<#
.SYNOPSIS
    Switches the active warehouse schema and restarts the Dagster containers.

.DESCRIPTION
    Updates WAREHOUSE_SCHEMA in .env, then runs docker compose down and docker compose up -d
    so the containers pick up the new value. After this completes, go to the Dagster UI and
    re-materialize warehouse_mode_select to confirm the change took effect.

.PARAMETER Schema
    The target schema: dev, test, or prod.

.EXAMPLE
    .\Set-WarehouseSchema.ps1 dev
    .\Set-WarehouseSchema.ps1 test
    .\Set-WarehouseSchema.ps1 prod
#>

param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("dev", "test", "prod")]
    [string]$Schema
)

$EnvFile = Join-Path $PSScriptRoot ".env"

if (-not (Test-Path $EnvFile)) {
    Write-Error "Could not find .env at: $EnvFile"
    exit 1
}

# Read current value for display
$lines    = [System.IO.File]::ReadAllLines($EnvFile)
$current  = ($lines | Where-Object { $_ -match "^WAREHOUSE_SCHEMA=" }) -replace "^WAREHOUSE_SCHEMA=", ""

if ($current -eq $Schema) {
    Write-Host "WAREHOUSE_SCHEMA is already '$Schema' - nothing to change." -ForegroundColor Cyan
    exit 0
}

Write-Host ""
Write-Host "Switching warehouse schema:  $current  ->  $Schema" -ForegroundColor Yellow
Write-Host ""

# Update .env — replace existing line or append if missing
$found   = $false
$updated = $lines | ForEach-Object {
    if ($_ -match "^WAREHOUSE_SCHEMA=") {
        $found = $true
        "WAREHOUSE_SCHEMA=$Schema"
    } else {
        $_
    }
}
if (-not $found) {
    $updated += "WAREHOUSE_SCHEMA=$Schema"
}

[System.IO.File]::WriteAllLines($EnvFile, $updated)
Write-Host "Step 1/3  Updated .env" -ForegroundColor Green

# Restart containers
Write-Host "Step 2/3  Stopping containers..." -ForegroundColor Green
docker compose --project-directory $PSScriptRoot down

Write-Host "Step 3/3  Starting containers..." -ForegroundColor Green
docker compose --project-directory $PSScriptRoot up -d

Write-Host ""
Write-Host "Done. Warehouse schema is now '$Schema'." -ForegroundColor Green
Write-Host "Go to the Dagster UI and re-materialize warehouse_mode_select to confirm." -ForegroundColor Cyan
Write-Host ""
