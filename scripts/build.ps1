<#
.SYNOPSIS
    Build all projects in Release configuration.

.PARAMETER Configuration
    Build configuration (default: Release).

.PARAMETER Verbosity
    MSBuild verbosity: quiet, minimal, normal, detailed, diagnostic (default: minimal).

.EXAMPLE
    .\build.ps1
    .\build.ps1 -Configuration Debug
    .\build.ps1 -Verbosity normal
#>
param(
    [string] $Configuration = "Release",
    [string] $Verbosity     = "minimal"
)

$ErrorActionPreference = "Stop"

$Solution = Join-Path (Split-Path $PSScriptRoot -Parent) "NymBroker.slnx"

Write-Host "Building $Configuration..."
dotnet build $Solution --configuration $Configuration --verbosity $Verbosity
if ($LASTEXITCODE -ne 0) { throw "Build failed." }

Write-Host "Build succeeded ($Configuration)."
