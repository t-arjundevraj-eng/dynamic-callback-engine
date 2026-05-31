# Kafka Spring Boot High-Throughput Demo

This application starts 50-100 logical producers from a bounded producer pool, sends large volumes of JSON events to Kafka, consumes them with a concurrent Kafka consumer pool, and writes consumed messages to MySQL in batches.

## Prerequisites

- Java 8 is enough for this project.
- Maven is required, or use the IntelliJ bundled Maven through `scripts/run-app.ps1`.
- Kafka must be reachable at `localhost:9092`.
- MySQL must be reachable at `localhost:3306`, with database `kafka_demo`.

If Docker is installed, start Kafka and MySQL with:

```powershell
docker compose up -d
```

If Docker is not installed, create the MySQL database manually:

```sql
CREATE DATABASE IF NOT EXISTS kafka_demo;
```

## Run the app on Windows

```powershell
.\scripts\run-app.ps1
```

The default MySQL username/password are `root`/`root`. Override them if needed:

```powershell
.\scripts\run-app.ps1 -MysqlUser root -MysqlPassword "your-password"
```

## Start a load job

This sends 5,000,000 messages:

```powershell
.\scripts\start-load.ps1 -Producers 50 -MessagesPerProducer 100000 -PayloadBytes 256
```

## Check job status

```powershell
.\scripts\check-load.ps1 -JobId "paste-job-id-here"
```

## Vendor ingestion

The app also supports schema-driven vendor ingestion. Configure vendors in `src/main/resources/application.yml` under `app.vendor-ingestion.vendors`.

For each configured vendor, the app creates:

```text
<vendor>.raw
<vendor>.retry
<vendor>.dlq
```

Valid messages are written idempotently to `vendor_events`. Validation failures go directly to the vendor DLQ and are also stored in `vendor_dead_letters`. Database/write failures are republished to `<vendor>.retry` with an `x-retry-count` header, then moved to DLQ after `app.vendor-ingestion.max-retry-attempts`.

Send a vendor event:

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8080/api/vendors/vendor-a/events" `
  -ContentType "application/json" `
  -Body '{"eventId":"a-1","customerId":"c-1","amount":100.25}'
```

Send a batch:

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://localhost:8080/api/vendors/vendor-b/events/batch" `
  -ContentType "application/json" `
  -Body '[{"messageId":"b-1","accountNumber":"acc-1","status":"NEW"}]'
```

## Important tuning notes

- Kafka topic partitions should be at least as high as the desired active consumer threads.
- `app.producer.max-pool-size` controls the maximum number of concurrent producer tasks.
- `app.kafka.consumer-concurrency` controls the consumer pool size.
- `app.vendor-ingestion.consumer-concurrency` controls the vendor ingestion consumer pool size.
- `app.vendor-ingestion.max-retry-attempts` controls how many retry-topic attempts happen before DLQ.
- `spring.kafka.consumer.max-poll-records` controls how many records each consumer thread writes to MySQL per batch.
- The consumer uses MySQL `INSERT IGNORE` plus JDBC batching, so Kafka retry duplicates do not fail the whole batch.
- Vendor ingestion uses `(vendor_name, event_id)` as the idempotency key.
- For serious million-message tests, do not run Kafka, MySQL, and the app all on a very small machine.
