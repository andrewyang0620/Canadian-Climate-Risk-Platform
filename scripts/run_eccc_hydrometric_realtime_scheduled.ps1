[CmdletBinding()]
param(
    [string]$ProjectRoot = "",

    [string]$PythonExecutable = "python",

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


function Write-JsonFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,

        [Parameter(Mandatory = $true)]
        [object]$Payload
    )

    $parent = Split-Path -Parent $Path
    New-Item -ItemType Directory -Force $parent | Out-Null

    $Payload |
        ConvertTo-Json -Depth 10 |
        Set-Content -Encoding UTF8 $Path
}


function Remove-OldExtractDateDirectories {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root,

        [Parameter(Mandatory = $true)]
        [datetime]$CutoffUtc
    )

    $removed = @()

    if (-not (Test-Path $Root)) {
        return $removed
    }

    Get-ChildItem $Root -Directory -Filter "extract_date=*" |
        ForEach-Object {
            $dateText = $_.Name.Replace("extract_date=", "")

            try {
                $partitionDate = [datetime]::ParseExact(
                    $dateText,
                    "yyyy-MM-dd",
                    [System.Globalization.CultureInfo]::InvariantCulture,
                    [System.Globalization.DateTimeStyles]::AssumeUniversal
                )

                if ($partitionDate.Date -lt $CutoffUtc.Date) {
                    $removed += $_.FullName
                    Remove-Item $_.FullName -Recurse -Force
                }
            }
            catch {
                Write-Warning (
                    "Skipping unrecognized extract-date directory: " +
                    $_.FullName
                )
            }
        }

    return $removed
}


function Remove-OldItemsByModifiedTime {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root,

        [Parameter(Mandatory = $true)]
        [datetime]$CutoffUtc
    )

    $removed = @()

    if (-not (Test-Path $Root)) {
        return $removed
    }

    Get-ChildItem $Root -Force |
        Where-Object {
            $_.LastWriteTimeUtc -lt $CutoffUtc
        } |
        ForEach-Object {
            $removed += $_.FullName
            Remove-Item $_.FullName -Recurse -Force
        }

    return $removed
}


$startedAtUtc = [datetime]::UtcNow
$timestamp = $startedAtUtc.ToString("yyyyMMddTHHmmssZ")

$logRoot = Join-Path `
    $ProjectRoot `
    "lakehouse\_pipeline_logs\eccc_hydrometric_realtime"

$stateRoot = Join-Path `
    $ProjectRoot `
    "lakehouse\_pipeline_state\eccc_hydrometric_realtime"

$alertRoot = Join-Path `
    $ProjectRoot `
    "lakehouse\_alerts\eccc_hydrometric_realtime"

$pipelineReportRoot = Join-Path `
    $ProjectRoot `
    "lakehouse\_pipeline_runs\eccc_hydrometric_realtime"

New-Item -ItemType Directory -Force $logRoot | Out-Null
New-Item -ItemType Directory -Force $stateRoot | Out-Null
New-Item -ItemType Directory -Force $alertRoot | Out-Null

$stdoutPath = Join-Path $logRoot "$timestamp.stdout.log"
$stderrPath = Join-Path $logRoot "$timestamp.stderr.log"
$latestStatePath = Join-Path $stateRoot "latest_run.json"

$exitCode = 1
$pythonPath = $null

try {
    $pythonPath = (
        Get-Command $PythonExecutable -ErrorAction Stop
    ).Source

    $process = Start-Process `
        -FilePath $pythonPath `
        -ArgumentList @(
            "-m",
            "src.pipelines.run_eccc_hydrometric_realtime"
        ) `
        -WorkingDirectory $ProjectRoot `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath `
        -Wait `
        -PassThru `
        -NoNewWindow

    $exitCode = $process.ExitCode
    $completedAtUtc = [datetime]::UtcNow

    if (Test-Path $stdoutPath) {
        Get-Content $stdoutPath
    }

    if (
        (Test-Path $stderrPath) -and
        ((Get-Item $stderrPath).Length -gt 0)
    ) {
        Get-Content $stderrPath | Write-Warning
    }

    if ($exitCode -ne 0) {
        throw "Realtime pipeline exited with code $exitCode."
    }

    $latestPipelineReport = Get-ChildItem `
        $pipelineReportRoot `
        -Recurse `
        -Filter "pipeline_report.json" `
        -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1

    $cutoffUtc = [datetime]::UtcNow.AddDays(-$RetentionDays)

    $removedItems = @()

    $removedItems += Remove-OldExtractDateDirectories `
        -Root (
            Join-Path `
                $ProjectRoot `
                "lakehouse\bronze\eccc_hydrometric_realtime"
        ) `
        -CutoffUtc $cutoffUtc

    $removedItems += Remove-OldExtractDateDirectories `
        -Root (
            Join-Path `
                $ProjectRoot `
                "lakehouse\silver\silver_hydro_realtime_observation"
        ) `
        -CutoffUtc $cutoffUtc

    $removedItems += Remove-OldExtractDateDirectories `
        -Root (
            Join-Path `
                $ProjectRoot `
                "lakehouse\silver\_metadata\eccc_hydro_realtime_observation"
        ) `
        -CutoffUtc $cutoffUtc

    $removedItems += Remove-OldItemsByModifiedTime `
        -Root $pipelineReportRoot `
        -CutoffUtc $cutoffUtc

    $state = [ordered]@{
        pipeline_name = "eccc_hydrometric_realtime_pipeline"
        status = "success"
        started_at_utc = $startedAtUtc.ToString("o")
        completed_at_utc = $completedAtUtc.ToString("o")
        duration_seconds = [math]::Round(
            ($completedAtUtc - $startedAtUtc).TotalSeconds,
            3
        )
        exit_code = $exitCode
        python_executable = $pythonPath
        project_root = $ProjectRoot
        stdout_path = $stdoutPath
        stderr_path = $stderrPath
        pipeline_report_path = if ($latestPipelineReport) {
            $latestPipelineReport.FullName
        }
        else {
            $null
        }
        retention_days = $RetentionDays
        removed_item_count = $removedItems.Count
        removed_items = $removedItems
    }

    Write-JsonFile `
        -Path $latestStatePath `
        -Payload $state

    Write-Host (
        "[OK] Scheduled ECCC realtime pipeline completed | " +
        "duration_seconds=$($state.duration_seconds) " +
        "removed_items=$($state.removed_item_count)"
    )

    exit 0
}
catch {
    $completedAtUtc = [datetime]::UtcNow

    $failureState = [ordered]@{
        pipeline_name = "eccc_hydrometric_realtime_pipeline"
        status = "failed"
        started_at_utc = $startedAtUtc.ToString("o")
        completed_at_utc = $completedAtUtc.ToString("o")
        duration_seconds = [math]::Round(
            ($completedAtUtc - $startedAtUtc).TotalSeconds,
            3
        )
        exit_code = $exitCode
        python_executable = $pythonPath
        project_root = $ProjectRoot
        stdout_path = $stdoutPath
        stderr_path = $stderrPath
        error_type = $_.Exception.GetType().FullName
        error_message = $_.Exception.Message
    }

    Write-JsonFile `
        -Path $latestStatePath `
        -Payload $failureState

    $alertPath = Join-Path $alertRoot "$timestamp.failure.json"

    Write-JsonFile `
        -Path $alertPath `
        -Payload $failureState

    Write-Error (
        "Scheduled ECCC realtime pipeline failed. " +
        "Alert written to $alertPath"
    )

    if ($exitCode -eq 0) {
        exit 1
    }

    exit $exitCode
}
