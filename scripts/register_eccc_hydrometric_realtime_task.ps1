[CmdletBinding()]
param(
    [string]$ProjectRoot = "",

    [string]$TaskName = (
        "CanadianClimateRisk-" +
        "ECCC-Hydrometric-Realtime"
    ),

    [int]$RetentionDays = 7
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"


if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
    $scriptPath = $PSCommandPath

    if ([string]::IsNullOrWhiteSpace($scriptPath)) {
        $scriptPath = $MyInvocation.MyCommand.Path
    }

    if ([string]::IsNullOrWhiteSpace($scriptPath)) {
        throw (
            "Unable to determine the script location. " +
            "Pass -ProjectRoot explicitly."
        )
    }

    $scriptDirectory = Split-Path -Parent $scriptPath

    $ProjectRoot = (
        Resolve-Path (Join-Path $scriptDirectory "..")
    ).Path
}

$runnerPath = Join-Path `
    $ProjectRoot `
    "scripts\run_eccc_hydrometric_realtime_scheduled.ps1"

if (-not (Test-Path $runnerPath)) {
    throw "Scheduled runner not found: $runnerPath"
}

$pythonPath = (Get-Command python -ErrorAction Stop).Source

$arguments = @(
    "-NoProfile"
    "-ExecutionPolicy Bypass"
    "-File `"$runnerPath`""
    "-ProjectRoot `"$ProjectRoot`""
    "-PythonExecutable `"$pythonPath`""
    "-RetentionDays $RetentionDays"
) -join " "

$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument $arguments `
    -WorkingDirectory $ProjectRoot

$triggerTimes = @(
    [datetime]::Today.AddMinutes(20),
    [datetime]::Today.AddHours(6).AddMinutes(20),
    [datetime]::Today.AddHours(12).AddMinutes(20),
    [datetime]::Today.AddHours(18).AddMinutes(20)
)

$triggers = foreach ($triggerTime in $triggerTimes) {
    New-ScheduledTaskTrigger `
        -Daily `
        -At $triggerTime
}

$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2) `
    -RestartCount 2 `
    -RestartInterval (New-TimeSpan -Minutes 15)

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $triggers `
    -Settings $settings `
    -Description (
        "Runs the ECCC hydrometric realtime Bronze, Silver, " +
        "validation, logging, and retention workflow every six hours."
    ) `
    -Force | Out-Null

Write-Host "[OK] Registered scheduled task: $TaskName"
Write-Host "Python: $pythonPath"
Write-Host "Runner: $runnerPath"
Write-Host "Schedule: 00:20, 06:20, 12:20, 18:20 local time"
