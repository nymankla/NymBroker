# Starts PostgreSQL in Docker and waits until healthy.
# Requires Docker Desktop to be running.

param(
    [switch]$Stop,
    [switch]$Logs
)

$container = "messagebroker-postgres"

if ($Stop) {
    Write-Host "Stopping PostgreSQL..." -ForegroundColor Yellow
    docker compose down
    exit 0
}

if ($Logs) {
    docker logs -f $container
    exit 0
}

Write-Host "Starting PostgreSQL..." -ForegroundColor Cyan
docker compose up -d postgres

Write-Host "Waiting for PostgreSQL to become healthy..." -ForegroundColor Yellow
$attempts = 0
do {
    Start-Sleep -Seconds 2
    $status = docker inspect --format='{{.State.Health.Status}}' $container 2>$null
    $attempts++
    if ($attempts -gt 30) {
        Write-Host "Timed out waiting for PostgreSQL." -ForegroundColor Red
        exit 1
    }
} while ($status -ne "healthy")

Write-Host ""
Write-Host "PostgreSQL is ready!" -ForegroundColor Green
Write-Host "  Host:     localhost:5432"
Write-Host "  Database: nymbroker"
Write-Host "  User:     postgres / postgres"
