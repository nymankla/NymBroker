# Starts ElasticMQ (an SQS-compatible server) in Docker and waits until it answers.
# Requires Docker Desktop to be running.

param(
    [switch]$Stop,
    [switch]$Logs
)

$container   = "messagebroker-elasticmq"
$service     = "elasticmq"
$url         = "http://localhost:9324"
$ComposeFile = Join-Path $PSScriptRoot "docker-compose.yml"

if ($Stop) {
    # Stop only ElasticMQ so the other services keep running.
    Write-Host "Stopping ElasticMQ..." -ForegroundColor Yellow
    docker compose -f $ComposeFile stop $service
    exit 0
}

if ($Logs) {
    docker logs -f $container
    exit 0
}

Write-Host "Starting ElasticMQ..." -ForegroundColor Cyan
docker compose -f $ComposeFile up -d $service
if ($LASTEXITCODE -ne 0) { exit 1 }

# The native image has no shell for a Docker health check, so poll the SQS API from the host.
Write-Host "Waiting for ElasticMQ to answer..." -ForegroundColor Yellow
$attempts = 0
while ($true) {
    try {
        Invoke-WebRequest -Uri "$url/?Action=ListQueues" -UseBasicParsing -TimeoutSec 2 | Out-Null
        break
    }
    catch {
        $attempts++
        if ($attempts -gt 30) {
            Write-Host "Timed out waiting for ElasticMQ. Check: .\setup-sqs.ps1 -Logs" -ForegroundColor Red
            exit 1
        }
        Start-Sleep -Seconds 1
    }
}

Write-Host ""
Write-Host "ElasticMQ is ready!" -ForegroundColor Green
Write-Host "  SQS API: $url   (any access key / secret; region us-east-1)"
Write-Host "  Web UI:  http://localhost:9325"
Write-Host "  Tests:   `$env:NYMBROKER_SQS_URL = `"$url`""
