param(
    [string]$LogFile
)

$ErrorActionPreference = "Stop"

if (-not $LogFile) {
    $logDir = Join-Path $PSScriptRoot "..\logs"
    if (!(Test-Path $logDir)) {
        Write-Host "No logs folder yet. Start the app with .\scripts\run-app.ps1 first."
        exit 1
    }
    $LogFile = Get-ChildItem -Path $logDir -Filter "app-*.log" |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1 -ExpandProperty FullName
}

if (-not $LogFile -or !(Test-Path $LogFile)) {
    Write-Host "No log file found."
    exit 1
}

Write-Host "Log: $LogFile"
Write-Host ""

$patterns = @(
    "Refreshed .* resolved vendor callback",
    "Active callback queues:",
    "Vendor callback polling manager started",
    "Skipping queue",
    "Scheduled Kafka-publish poller",
    "Publishing .* row",
    "Vendor callback succeeded",
    "Vendor callback failed",
    "ERROR",
    "Exception",
    "Unknown column"
)

foreach ($pattern in $patterns) {
    $matches = Select-String -Path $LogFile -Pattern $pattern -CaseSensitive:$false
    if ($matches) {
        Write-Host "=== $pattern ===" -ForegroundColor Yellow
        $matches | ForEach-Object { Write-Host $_.Line }
        Write-Host ""
    }
}

$kafkaNoise = (Select-String -Path $LogFile -Pattern "partitions assigned" -SimpleMatch).Count
if ($kafkaNoise -gt 0) {
    Write-Host "(Suppressed $kafkaNoise 'partitions assigned' lines — use the log file if you need them.)" -ForegroundColor DarkGray
}
