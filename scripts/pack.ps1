<#
.SYNOPSIS
    Pack all NymBroker library projects into NuGet packages.

.DESCRIPTION
    Reads the current version from Directory.Build.props.
    Optionally bumps or overrides the version before packing.
    Outputs .nupkg files to artifacts/nupkg/, and zips the client skills in skills/
    into artifacts/nymbroker-skills.zip.

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

.PARAMETER SkillsZip
    Path of the client skills zip (default: artifacts/nymbroker-skills.zip).

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
    [string] $SkillsZip     = "artifacts/nymbroker-skills.zip",
    [string] $Configuration = "Release"
)

$ErrorActionPreference = "Stop"

$Root      = Split-Path $PSScriptRoot -Parent
$PropsFile = Join-Path $Root "Directory.Build.props"

# ── read current version ────────────────────────────────────────────────────
[xml]$props   = Get-Content $PropsFile -Encoding UTF8
$versionNode  = $props.SelectSingleNode("/Project/PropertyGroup/Version")
$current      = $versionNode.InnerText.Trim()
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
    $versionNode.InnerText = $Version
    $props.Save($PropsFile)
    Write-Host "Version updated: $current -> $Version"
} else {
    $Version = $current
    Write-Host "Packing version $Version  (use -Version / -BumpPatch / -BumpMinor / -BumpMajor to change)"
}

# ── projects to pack ────────────────────────────────────────────────────────
# Every top-level NymBroker.* project whose IsPackable is true (test projects set it false).
# Discovered rather than hard-coded so a new library project that another package depends on
# can't be left out and publish a package nobody can restore.
$Projects = Get-ChildItem $Root -Directory -Filter "NymBroker.*" |
    ForEach-Object { Get-ChildItem $_.FullName -Filter "*.csproj" } |
    Where-Object {
        $packable = dotnet msbuild $_.FullName -getProperty:IsPackable
        if ($LASTEXITCODE -ne 0) { throw "Could not evaluate $($_.Name)." }
        "$packable".Trim() -eq "true"
    } |
    Sort-Object Name

if (-not $Projects) { throw "No packable NymBroker.* projects found under $Root." }

# Path.Combine keeps an absolute -OutputDir (e.g. C:\feed) as-is instead of nesting it under $Root.
$Out = [System.IO.Path]::Combine($Root, $OutputDir)
New-Item -ItemType Directory -Force -Path $Out | Out-Null

# ── build, then pack ────────────────────────────────────────────────────────
Write-Host ""
foreach ($proj in $Projects) {
    Write-Host "Building $($proj.Name)..."
    dotnet build $proj.FullName --configuration $Configuration -v q
    if ($LASTEXITCODE -ne 0) { throw "Build failed for $($proj.Name)." }
}

foreach ($proj in $Projects) {
    Write-Host "Packing $($proj.Name)..."
    dotnet pack $proj.FullName `
        --configuration $Configuration `
        --no-build `
        --output $Out `
        -v q
    if ($LASTEXITCODE -ne 0) { throw "dotnet pack failed for $($proj.Name)" }
}

# ── verify NymBroker.* dependencies were packed too ─────────────────────────
Add-Type -AssemblyName System.IO.Compression.FileSystem
$missing = @()
foreach ($proj in $Projects) {
    $id      = dotnet msbuild $proj.FullName -getProperty:PackageId
    $nupkg   = Join-Path $Out "$("$id".Trim()).$Version.nupkg"
    if (-not (Test-Path $nupkg)) { $missing += "$nupkg (expected from $($proj.Name))"; continue }

    $zip = [System.IO.Compression.ZipFile]::OpenRead($nupkg)
    try {
        $entry  = $zip.Entries | Where-Object { $_.FullName -like "*.nuspec" } | Select-Object -First 1
        $reader = New-Object System.IO.StreamReader($entry.Open())
        [xml]$nuspec = $reader.ReadToEnd()
        $reader.Dispose()
    } finally { $zip.Dispose() }

    $deps = $nuspec.SelectNodes("//*[local-name()='dependency']") |
        Where-Object { $_.id -like "NymBroker*" } |
        Select-Object -ExpandProperty id -Unique
    foreach ($dep in $deps) {
        if (-not (Test-Path (Join-Path $Out "$dep.$Version.nupkg"))) {
            $missing += "$dep.$Version.nupkg (dependency of $("$id".Trim()))"
        }
    }
}
if ($missing) { throw "Missing packages:`n  $($missing -join "`n  ")" }

# ── client skills zip ───────────────────────────────────────────────────────
# One folder per skill at the zip root, so it extracts straight into .claude/skills/.
# Entries are written with '/' separators so the zip also extracts correctly on Linux and macOS.
$SkillsDir = Join-Path $Root "skills"
$ZipPath   = [System.IO.Path]::Combine($Root, $SkillsZip)
$skillFolders = Get-ChildItem $SkillsDir -Directory | Where-Object { Test-Path (Join-Path $_.FullName "SKILL.md") }
if (-not $skillFolders) { throw "No skills found under $SkillsDir." }

New-Item -ItemType Directory -Force -Path (Split-Path $ZipPath -Parent) | Out-Null
if (Test-Path $ZipPath) { Remove-Item $ZipPath -Force }
$archive = [System.IO.Compression.ZipFile]::Open($ZipPath, [System.IO.Compression.ZipArchiveMode]::Create)
try {
    foreach ($folder in $skillFolders) {
        Get-ChildItem $folder.FullName -File -Recurse | ForEach-Object {
            $relative = $_.FullName.Substring($SkillsDir.Length).TrimStart([char]'\', [char]'/') -replace '\\', '/'
            [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile($archive, $_.FullName, $relative) | Out-Null
        }
    }
} finally { $archive.Dispose() }

# ── summary ─────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "Packages written to: $Out"
Get-ChildItem $Out -Filter "*.$Version.nupkg" | Sort-Object Name | ForEach-Object {
    Write-Host "  $($_.Name)"
}
Write-Host "Skills zip: $ZipPath ($($skillFolders.Name -join ', '))"
