# Starts RabbitMQ in Docker and waits until healthy.
# Requires Docker Desktop to be running.

param(
    [switch]$Stop,
    [switch]$Logs
)

$container   = "messagebroker-rabbitmq"
$ComposeFile = Join-Path $PSScriptRoot "docker-compose.yml"

if ($Stop) {
    Write-Host "Stopping RabbitMQ..." -ForegroundColor Yellow
    docker compose -f $ComposeFile down
    exit 0
}

if ($Logs) {
    docker logs -f $container
    exit 0
}

Write-Host "Starting RabbitMQ..." -ForegroundColor Cyan
docker compose -f $ComposeFile up -d rabbitmq

Write-Host "Waiting for RabbitMQ to become healthy..." -ForegroundColor Yellow
$attempts = 0
do {
    Start-Sleep -Seconds 2
    $status = docker inspect --format='{{.State.Health.Status}}' $container 2>$null
    $attempts++
    if ($attempts -gt 30) {
        Write-Host "Timed out waiting for RabbitMQ." -ForegroundColor Red
        exit 1
    }
} while ($status -ne "healthy")

Write-Host ""
Write-Host "RabbitMQ is ready!" -ForegroundColor Green
Write-Host "  AMQP:       amqp://guest:guest@localhost:5672"
Write-Host "  Management: http://localhost:15672  (guest / guest)"
