param(
    [int]$Producers = 50,
    [long]$MessagesPerProducer = 100000,
    [int]$PayloadBytes = 256,
    [string]$BaseUrl = "http://localhost:8080"
)

$ErrorActionPreference = "Stop"

$body = @{
    producers = $Producers
    messagesPerProducer = $MessagesPerProducer
    payloadBytes = $PayloadBytes
} | ConvertTo-Json

Invoke-RestMethod `
    -Method Post `
    -Uri "$BaseUrl/api/load/start" `
    -ContentType "application/json" `
    -Body $body
