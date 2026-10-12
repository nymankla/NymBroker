# Runs every test, including the PostgreSQL / SQL Server / Service Bus / RabbitMQ integration tests:
# makes sure Docker Desktop is running, starts the containers, sets the NYMBROKER_* variables and runs dotnet test.
# Containers are left running (stop them with the setup scripts' -Stop).
#
#   ./scripts/test-all.ps1
#   ./scripts/test-all.ps1 -Filter "*RabbitMqEndPointTests"
#   ./scripts/test-all.ps1 -SkipInfra          # containers already up

param(
    [string]$Filter,
    [switch]$SkipInfra
)

$root = Split-Path $PSScriptRoot -Parent

function Test-Docker { docker info *> $null; return $LASTEXITCODE -eq 0 }

if (-not $SkipInfra) {
    if (-not (Test-Docker)) {
        $desktop = "C:\Program Files\Docker\Docker\Docker Desktop.exe"
        if (-not (Test-Path $desktop)) { Write-Host "Docker is not running and Docker Desktop was not found." -ForegroundColor Red; exit 1 }
        Write-Host "Starting Docker Desktop..." -ForegroundColor Cyan
        Start-Process $desktop
        $deadline = (Get-Date).AddMinutes(3)
        while (-not (Test-Docker)) {
            if ((Get-Date) -gt $deadline) { Write-Host "Docker did not start within 3 minutes." -ForegroundColor Red; exit 1 }
            Start-Sleep -Seconds 3
        }
    }

    # Never the -Stop variants here: setup-rabbitmq/-postgres -Stop take down every service.
    # setup-servicebus also starts SQL Server, so SQL Server first (it creates the nymbroker database).
    foreach ($script in "setup-sqlserver.ps1", "setup-servicebus.ps1", "setup-postgres.ps1", "setup-rabbitmq.ps1") {
        & (Join-Path $PSScriptRoot $script)
        if ($LASTEXITCODE -ne 0) { Write-Host "$script failed." -ForegroundColor Red; exit 1 }
    }
}

$env:NYMBROKER_POSTGRES_CS   = "Host=localhost;Database=nymbroker;Username=postgres;Password=postgres"
$env:NYMBROKER_SQLSERVER_CS  = "Server=localhost,1433;Database=nymbroker;User Id=sa;Password=NymBroker!Dev123;TrustServerCertificate=True"
$env:NYMBROKER_SERVICEBUS_CS = "Endpoint=sb://localhost:5673;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
$env:NYMBROKER_RABBITMQ_HOST = "localhost"

Push-Location $root
try {
    if ($Filter) { dotnet test --project NymBroker.Tests -- --filter-class $Filter }
    else { dotnet test }
    exit $LASTEXITCODE
}
finally { Pop-Location }
