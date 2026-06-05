param(
    [string]$MysqlUser = "root",
    [string]$MysqlPassword = "root",
    [string]$MysqlUrl = "jdbc:mysql://localhost:3306/kafka_demo?rewriteBatchedStatements=true&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
    [string]$KafkaBootstrapServers = "localhost:9092",
    [switch]$NoLogFile
)

$ErrorActionPreference = "Stop"

$mvn = (Get-Command mvn -ErrorAction SilentlyContinue)
if ($mvn) {
    $mvnPath = $mvn.Source
} else {
    $mvnPath = "C:\Program Files\JetBrains\IntelliJ IDEA 2026.1.2\plugins\maven\lib\maven3\bin\mvn.cmd"
}

if (!(Test-Path $mvnPath)) {
    throw "Maven was not found on PATH and IntelliJ bundled Maven was not found at $mvnPath"
}

$env:SPRING_DATASOURCE_URL = $MysqlUrl
$env:SPRING_DATASOURCE_USERNAME = $MysqlUser
$env:SPRING_DATASOURCE_PASSWORD = $MysqlPassword
$env:SPRING_KAFKA_BOOTSTRAP_SERVERS = $KafkaBootstrapServers

$logFile = $null
if (-not $NoLogFile) {
    $logDir = Join-Path $PSScriptRoot "..\logs"
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    $logFile = Join-Path $logDir ("app-{0:yyyyMMdd-HHmmss}.log" -f (Get-Date))
    Write-Host ""
    Write-Host "Full output is also written to:" -ForegroundColor Cyan
    Write-Host "  $logFile" -ForegroundColor Cyan
    Write-Host "After startup, run:  .\scripts\show-callback-status.ps1" -ForegroundColor Cyan
    Write-Host ""
}

if ($logFile) {
    & $mvnPath spring-boot:run 2>&1 | Tee-Object -FilePath $logFile
} else {
    & $mvnPath spring-boot:run
}
