# Starts MySQL 8.4 in Docker and waits until healthy. The image creates the nymbroker database on first start.
# Requires Docker Desktop to be running.

param(
    [switch]$Stop,
    [switch]$Logs
)

$container   = "messagebroker-mysql"
$service     = "mysql"
$ComposeFile = Join-Path $PSScriptRoot "docker-compose.yml"

if ($Stop) {
    # Stop only MySQL so the other services keep running.
    Write-Host "Stopping MySQL..." -ForegroundColor Yellow
    docker compose -f $ComposeFile stop $service
    exit 0
}

if ($Logs) {
    docker logs -f $container
    exit 0
}

Write-Host "Starting MySQL..." -ForegroundColor Cyan
docker compose -f $ComposeFile up -d $service
if ($LASTEXITCODE -ne 0) { exit 1 }

Write-Host "Waiting for MySQL to become healthy (first start can take a minute)..." -ForegroundColor Yellow
$attempts = 0
do {
    Start-Sleep -Seconds 2
    $status = docker inspect --format='{{.State.Health.Status}}' $container 2>$null
    $attempts++
    if ($attempts -gt 60) {
        Write-Host "Timed out waiting for MySQL. Check: .\setup-mysql.ps1 -Logs" -ForegroundColor Red
        exit 1
    }
} while ($status -ne "healthy")

Write-Host ""
Write-Host "MySQL is ready!" -ForegroundColor Green
Write-Host "  Host:     localhost:3306"
Write-Host "  Database: nymbroker"
Write-Host "  User:     root / NymBroker!Dev123"
Write-Host "  Connection string:"
Write-Host "    Server=localhost;Port=3306;Database=nymbroker;User ID=root;Password=NymBroker!Dev123"
