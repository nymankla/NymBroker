<#
.SYNOPSIS
    Pack all NymBroker library projects into NuGet packages.

.DESCRIPTION
    Reads the current version from Directory.Build.props.
    Optionally bumps or overrides the version before packing.
    Outputs .nupkg files to artifacts/nupkg/.

.PARAMETER Version
    Set an explicit version (e.g. 1.2.3 or 1.2.3-preview.1).

.PARAMETER BumpPatch
    Increment the patch segment: 1.2.3 -> 1.2.4

.PARAMETER BumpMinor
    Increment the minor segment and reset patch: 1.2.3 -> 1.3.0

.PARAMETER BumpMajor
    Increment the major segment and reset minor/patch: 1.2.3 -> 2.0.0

.PARAMETER OutputDir
    Directory for .nupkg output (default: artifacts/nupkg).

.PARAMETER Configuration
    Build configuration (default: Release).

.EXAMPLE
    .\pack.ps1
    .\pack.ps1 -Version 1.0.0
    .\pack.ps1 -BumpPatch
    .\pack.ps1 -BumpMinor -OutputDir C:\feed
#>
param(
    [string] $Version       = "",
    [switch] $BumpPatch,
    [switch] $BumpMinor,
    [switch] $BumpMajor,
    [string] $OutputDir     = "artifacts/nupkg",
    [string] $Configuration = "Release"
)

$ErrorActionPreference = "Stop"

$Root      = $PSScriptRoot
$PropsFile = Join-Path $Root "Directory.Build.props"

# ── read current version ────────────────────────────────────────────────────
[xml]$props   = Get-Content $PropsFile -Encoding UTF8
$current      = $props.Project.PropertyGroup.Version
if (-not $current) { throw "No <Version> found in $PropsFile" }

# ── determine new version ───────────────────────────────────────────────────
$bumpCount = @($BumpMajor, $BumpMinor, $BumpPatch) | Where-Object { $_ } | Measure-Object | Select-Object -ExpandProperty Count
if ($bumpCount -gt 1) { throw "Specify at most one -Bump* flag." }
if ($bumpCount -gt 0 -and $Version) { throw "Cannot combine -Version with a -Bump* flag." }

if ($BumpMajor -or $BumpMinor -or $BumpPatch) {
    $parts = $current -split '\.'
    if ($parts.Count -lt 3) { throw "Current version '$current' is not in major.minor.patch format." }
    [int]$maj = $parts[0]; [int]$min = $parts[1]; [int]$pat = $parts[2]
    if     ($BumpMajor) { $maj++; $min = 0; $pat = 0 }
    elseif ($BumpMinor) { $min++;           $pat = 0 }
    else                {                   $pat++   }
    $Version = "$maj.$min.$pat"
}

if ($Version) {
    if ($Version -notmatch '^\d+\.\d+\.\d+') {
        throw "Version '$Version' must start with major.minor.patch (e.g. 1.2.3 or 1.2.3-preview.1)."
    }
    $props.Project.PropertyGroup.Version = $Version
    $props.Save($PropsFile)
    Write-Host "Version updated: $current -> $Version"
} else {
    $Version = $current
    Write-Host "Packing version $Version  (use -Version / -BumpPatch / -BumpMinor / -BumpMajor to change)"
}

# ── projects to pack ────────────────────────────────────────────────────────
$Projects = @(
    "NymBroker.Core/NymBroker.Core.csproj",
    "NymBroker.Sqlite/NymBroker.Sqlite.csproj",
    "NymBroker.Postgres/NymBroker.Postgres.csproj",
    "NymBroker.RabbitMq/NymBroker.RabbitMq.csproj"
)

$Out = Join-Path $Root $OutputDir
New-Item -ItemType Directory -Force -Path $Out | Out-Null

# ── build once, then pack ───────────────────────────────────────────────────
Write-Host ""
Write-Host "Building..."
dotnet build (Join-Path $Root "NymBroker.Core/NymBroker.Core.csproj") `
    --configuration $Configuration -v q
if ($LASTEXITCODE -ne 0) { throw "Build failed." }

foreach ($proj in $Projects) {
    $projPath = Join-Path $Root $proj
    Write-Host "Packing $proj..."
    dotnet pack $projPath `
        --configuration $Configuration `
        --no-build `
        --output $Out `
        -v q
    if ($LASTEXITCODE -ne 0) { throw "dotnet pack failed for $proj" }
}

# ── summary ─────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "Packages written to: $Out"
Get-ChildItem $Out -Filter "*.nupkg" | Sort-Object Name | ForEach-Object {
    Write-Host "  $($_.Name)"
}
