<#
.SYNOPSIS
    Build every project in the repository (Release by default), including samples the
    solution excludes for the chosen configuration. Fails if a project is missing from NymBroker.slnx.

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

$Root     = Split-Path $PSScriptRoot -Parent
$Solution = Join-Path $Root "NymBroker.slnx"
[xml]$slnx = Get-Content $Solution -Encoding UTF8

# ── every project on disk must be in the solution ───────────────────────────
$inSolution = $slnx.SelectNodes("//Project") | ForEach-Object { $_.Path -replace '\\', '/' }
$notInSolution = Get-ChildItem $Root -Recurse -Filter "*.csproj" |
    Where-Object { $_.FullName -notmatch '[\\/](bin|obj)[\\/]' } |
    ForEach-Object { $_.FullName.Substring($Root.Length).TrimStart('\', '/') -replace '\\', '/' } |
    Where-Object { $_ -notin $inSolution }
if ($notInSolution) { throw "Projects missing from NymBroker.slnx:`n  $($notInSolution -join "`n  ")" }

Write-Host "Building $Configuration..."
dotnet build $Solution --configuration $Configuration --verbosity $Verbosity
if ($LASTEXITCODE -ne 0) { throw "Build failed." }

# ── projects the solution skips for this configuration ──────────────────────
# NymBroker.slnx excludes some samples from Debug builds (<Build Solution="Debug|*" Project="false" />);
# build them explicitly so this script always builds everything.
$skipped = $slnx.SelectNodes("//Project") | Where-Object {
    $_.Build | Where-Object { $_.Project -eq "false" -and $_.Solution -like "$Configuration|*" }
}
foreach ($proj in $skipped) {
    Write-Host "Building $($proj.Path) (excluded from the $Configuration solution build)..."
    dotnet build (Join-Path $Root $proj.Path) --configuration $Configuration --verbosity $Verbosity
    if ($LASTEXITCODE -ne 0) { throw "Build failed for $($proj.Path)." }
}

Write-Host "Build succeeded ($Configuration)."
