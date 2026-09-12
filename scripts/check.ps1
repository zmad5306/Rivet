#!/usr/bin/env pwsh
param(
    [Alias('v')]
    [switch]$Verbose
)

# Native stderr is captured as diagnostics, including in Windows PowerShell 5.1.
$ErrorActionPreference = 'Continue'
$PSNativeCommandUseErrorActionPreference = $false

function Invoke-Check {
    param([string[]]$CargoArguments)

    if ($Verbose) {
        & cargo @CargoArguments
        $status = $LASTEXITCODE
    } else {
        $output = & cargo @CargoArguments 2>&1
        $status = $LASTEXITCODE
        if ($status -ne 0) {
            foreach ($line in $output) {
                [Console]::Error.WriteLine($line.ToString())
            }
        }
    }

    if ($status -ne 0) {
        exit $status
    }
}

$locationPushed = $false
try {
    $null = Get-Command cargo -CommandType Application -ErrorAction Stop
    Push-Location (Join-Path $PSScriptRoot '..') -ErrorAction Stop
    $locationPushed = $true

    Invoke-Check -CargoArguments @('fmt', '--check')
    Invoke-Check -CargoArguments @('clippy', '--all-targets', '--', '-D', 'warnings')
    Invoke-Check -CargoArguments @('test')

    Write-Host '✓ All checks passed.' -ForegroundColor Green
} catch {
    [Console]::Error.WriteLine($_.ToString())
    exit 1
} finally {
    if ($locationPushed) {
        Pop-Location
    }
}

exit 0
