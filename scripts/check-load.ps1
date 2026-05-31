param(
    [Parameter(Mandatory = $true)]
    [string]$JobId,
    [string]$BaseUrl = "http://localhost:8080"
)

$ErrorActionPreference = "Stop"

Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/load/$JobId"
