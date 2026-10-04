# Starts SQL Server in Docker, waits until healthy, and creates the nymbroker database.
# Requires Docker Desktop to be running.

param(
    [switch]$Stop,
    [switch]$Logs
)

$container   = "messagebroker-sqlserver"
$service     = "sqlserver"
$database    = "nymbroker"
$password    = "NymBroker!Dev123"   # must match MSSQL_SA_PASSWORD in docker-compose.yml
$ComposeFile = Join-Path $PSScriptRoot "docker-compose.yml"

if ($Stop) {
    # Stop only SQL Server so RabbitMQ / PostgreSQL keep running.
    Write-Host "Stopping SQL Server..." -ForegroundColor Yellow
    docker compose -f $ComposeFile stop $service
    exit 0
}

if ($Logs) {
    docker logs -f $container
    exit 0
}

Write-Host "Starting SQL Server..." -ForegroundColor Cyan
docker compose -f $ComposeFile up -d $service

Write-Host "Waiting for SQL Server to become healthy (first start can take a minute)..." -ForegroundColor Yellow
$attempts = 0
do {
    Start-Sleep -Seconds 2
    $status = docker inspect --format='{{.State.Health.Status}}' $container 2>$null
    $attempts++
    if ($attempts -gt 60) {
        Write-Host "Timed out waiting for SQL Server. Check: .\setup-sqlserver.ps1 -Logs" -ForegroundColor Red
        exit 1
    }
} while ($status -ne "healthy")

# Unlike the postgres image, the SQL Server image does not create a database from an env var.
Write-Host "Ensuring database '$database' exists..." -ForegroundColor Yellow
docker exec $container /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P $password -C -b `
    -Q "IF DB_ID(N'$database') IS NULL CREATE DATABASE [$database];"
if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to create database '$database'." -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "SQL Server is ready!" -ForegroundColor Green
Write-Host "  Host:     localhost,1433"
Write-Host "  Database: $database"
Write-Host "  User:     sa / $password"
Write-Host "  Connection string:"
Write-Host "    Server=localhost,1433;Database=$database;User Id=sa;Password=$password;TrustServerCertificate=True"
