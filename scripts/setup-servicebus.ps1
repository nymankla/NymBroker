# Starts the Azure Service Bus emulator in Docker (plus the SQL Server it stores state in) and waits until healthy.
# Entities (queues, topics, subscriptions) are declared in scripts/servicebus/Config.json.
# Requires Docker Desktop to be running. By starting it you accept the emulator EULA:
# https://github.com/Azure/azure-service-bus-emulator-installer/blob/main/EMULATOR_EULA.txt

param(
    [switch]$Stop,
    [switch]$Logs
)

$container   = "messagebroker-servicebus"
$service     = "servicebus"
$healthUrl   = "http://localhost:5300/health"
# AMQP is on host port 5673 (not 5672) so the emulator can run next to RabbitMQ.
$connectionString = "Endpoint=sb://localhost:5673;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
$ComposeFile = Join-Path $PSScriptRoot "docker-compose.yml"

if ($Stop) {
    # Stop only the emulator; SQL Server, RabbitMQ and PostgreSQL keep running.
    Write-Host "Stopping Service Bus emulator..." -ForegroundColor Yellow
    docker compose -f $ComposeFile stop $service
    exit 0
}

if ($Logs) {
    docker logs -f $container
    exit 0
}

Write-Host "Starting Service Bus emulator (and SQL Server, which it depends on)..." -ForegroundColor Cyan
docker compose -f $ComposeFile up -d $service
if ($LASTEXITCODE -ne 0) {
    Write-Host "docker compose failed." -ForegroundColor Red
    exit 1
}

Write-Host "Waiting for the emulator to become healthy (first start can take a minute)..." -ForegroundColor Yellow
$attempts = 0
do {
    Start-Sleep -Seconds 3
    try { $status = (Invoke-WebRequest -UseBasicParsing $healthUrl -TimeoutSec 3).StatusCode } catch { $status = 0 }
    $attempts++
    if ($attempts -gt 60) {
        Write-Host "Timed out waiting for the Service Bus emulator. Check: .\setup-servicebus.ps1 -Logs" -ForegroundColor Red
        exit 1
    }
} while ($status -ne 200)

Write-Host ""
Write-Host "Service Bus emulator is ready!" -ForegroundColor Green
Write-Host "  Entities: queues nymbroker.tests, nymbroker.bench, nymbroker.sample; topic nymbroker.tests.topic / subscription all"
Write-Host "  Connection string:"
Write-Host "    $connectionString"
