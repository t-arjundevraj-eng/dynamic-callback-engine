# Architecture — Dynamic Callback Engine

This document is the maintained technical reference for the `kafka-demo` application. See [README.md](README.md) for setup and operations.

---

## System context

```mermaid
C4Context
  title System context
  Person(ops, "Operator", "Configures queues and vendors")
  Person(ext, "External vendor", "Receives HTTP callbacks")
  System(app, "Dynamic Callback Engine", "Spring Boot :8080")
  SystemDb(mysql, "MySQL", "kafka_demo")
  System_Ext(kafka, "Apache Kafka", "Topics per queue")
  Rel(ops, app, "Configures")
  Rel(app, mysql, "JDBC")
  Rel(app, kafka, "Produce/consume")
  Rel(app, ext, "HTTP callbacks")
```

---

## Three subsystems

### A. Vendor callback gateway (primary)

**Goal:** Drain rows from per-vendor MySQL queue tables and invoke configured HTTP callbacks with validated, schema-driven payloads.

**Config sources:**

- `vendor_callback_queue_config` — operational knobs and pointers (`table_name`, `queue_name`)
- `sm_vendor_*` — business rules (URL, params, operators, packs)

**Entry point:** `VendorCallbackPollingManager` schedules one task per active, fully resolved queue.

#### Mode 1: Kafka dispatch (default)

`app.vendor-callback.dispatch-via-kafka: true`

```mermaid
sequenceDiagram
  participant Sched as VendorCallbackPollingManager
  participant Task as VendorQueueKafkaPublishTask
  participant DB as Source table
  participant K as Kafka queue_name
  participant Cons as VendorCallbackKafkaConsumer
  participant HTTP as VendorCallbackDispatcher
  participant Vendor as External vendor

  Sched->>Task: run on producer_sleep_time
  Task->>DB: SELECT NEW/RETRY LIMIT fetch_size
  Task->>Task: validate operator_id, pack_id, buildPayload
  Task->>K: VendorCallbackQueueMessage
  Task->>DB: bulkUpdate PUBLISHED or RETRY/DLQ
  K->>Cons: consume record
  Cons->>Cons: buildPayload
  Cons->>HTTP: dispatchAsync GET/POST
  HTTP->>Vendor: HTTP request
  Cons->>DB: bulkUpdate COMPLETED or RETRY/DLQ
```

| Step | Class | Responsibility |
|------|--------|----------------|
| 1 | `VendorConfigurationResolver` | JOIN config tables → `ResolvedVendorConfiguration` cache |
| 2 | `VendorCallbackSourceTableProvisioner` | `CREATE TABLE` if missing (standard columns + param fields) |
| 3 | `VendorSourceQueueJdbcRepository.pollUnprocessedRows` | `WHERE process_status IN ('NEW','RETRY')` |
| 4 | `VendorPayloadConstructionService` | Validate routing; map columns to JSON |
| 5 | `VendorCallbackKafkaPublisher` | Async publish to `queue_name` |
| 6 | `QueueRowStateTransitionService` | `PUBLISHED` on Kafka ACK; retry/DLQ on failure |
| 7 | `VendorCallbackKafkaConsumerManager` | One listener container per `queue_name` |
| 8 | `VendorCallbackKafkaConsumer` | Consume → HTTP → final state update |

#### Mode 2: Direct HTTP

`app.vendor-callback.dispatch-via-kafka: false`

Same poll/validate path, but `VendorCallbackPollerTask` calls `VendorCallbackDispatcher` directly (no `PUBLISHED` state).

```mermaid
flowchart LR
  Poll[VendorCallbackPollerTask] --> Val[PayloadConstruction]
  Val --> HTTP[VendorCallbackDispatcher]
  HTTP --> DB[(Update COMPLETED/RETRY/DLQ)]
```

---

### B. Vendor REST/Kafka ingestion

**Goal:** Accept events via REST, publish to Kafka, validate, and insert into the queue's `table_name`.

```mermaid
flowchart LR
  REST["POST /api/vendors/{vendor}/events"] --> PUB[VendorEventProducerService]
  PUB --> T1["Kafka: queue_name.ingest"]
  T1 --> L[DynamicVendorKafkaListenerManager]
  L --> C[VendorIngestionConsumer]
  C --> DB[(table_name INSERT IGNORE)]
  C -->|failure| RT[".ingest.retry / .ingest.dlq"]
  RT --> VDL[(vendor_dead_letters)]
```

| Component | Role |
|-----------|------|
| `VendorEventController` | REST API |
| `VendorTopicNames` | Topic naming: ingestion uses `.ingest` suffix |
| `VendorEventProducerService` | Publish `VendorEvent` to ingest topic |
| `DynamicVendorKafkaListenerManager` | Per-queue containers; `cons_pool_size`, `fetch_size` from config |
| `VendorIngestionConsumer` | Validate, save, retry/DLQ via `VendorFailurePublisher` |
| `VendorEventJdbcRepository` | Dynamic column-matched INSERT |

---

### C. Kafka load-test demo

**Goal:** Stress-test Kafka → MySQL write throughput.

```mermaid
flowchart LR
  API["POST /api/load/start"] --> LP[LoadProducerService]
  LP --> K[high-throughput-events]
  K --> TC[TelemetryConsumer]
  TC --> CE[(consumed_events)]
```

---

## Configuration resolution (callback)

```mermaid
erDiagram
  vendor_callback_queue_config ||--o{ sm_vendor_master : "vendor_name"
  sm_vendor_master ||--o{ sm_vendor_callback_config : "vendor_id"
  sm_vendor_master ||--o{ sm_vendor_operator_mapping : "vendor_id"
  sm_vendor_master ||--o{ sm_vendor_pack : "vendor_id"
  sm_vendor_master ||--o{ sm_vendor_param_configuration : "vendor_id"
  sm_vendor_master ||--o{ sm_vendor_ip_mapping : "vendor_id"

  vendor_callback_queue_config {
    int queue_id PK
    string queue_name "Kafka callback topic"
    string table_name "MySQL source table"
    string vendor_name
    int fetch_size
    bigint producer_sleep_time
    int cons_pool_size
    int max_retry_count
  }
```

**Resolver SQL** (`VendorConfigurationJdbcRepository`): inner join active queue + active vendor + non-empty callback URL + at least one active pack. Circle matching applies when `vendor_circle_flag = 1`.

---

## State machine (source queue rows)

```mermaid
stateDiagram-v2
  [*] --> NEW: INSERT row
  NEW --> PUBLISHED: Kafka publish OK
  NEW --> RETRY: publish or HTTP fail retry_count++
  NEW --> DLQ: validation fail or max retries
  RETRY --> PUBLISHED: Kafka publish OK
  RETRY --> RETRY: fail retry_count lt max
  RETRY --> DLQ: retry_count gte max
  PUBLISHED --> COMPLETED: HTTP OK
  PUBLISHED --> RETRY: HTTP fail
  PUBLISHED --> DLQ: HTTP fail max retries
  COMPLETED --> [*]
  DLQ --> [*]
```

Direct HTTP mode skips `PUBLISHED`.

---

## Threading model

| Pool / thread prefix | Used by |
|----------------------|---------|
| `vendor-callback-poller-*` | `ThreadPoolTaskScheduler` — one delayed task per queue |
| `vendor-callback-http-*` | `vendorCallbackDispatchExecutor` — Kafka publish + async HTTP |
| Kafka consumer threads | `VendorCallbackKafkaConsumerManager`, `DynamicVendorKafkaListenerManager` |
| `producer-pool-*` | `LoadProducerService` |
| Telemetry listener pool | `app.kafka.consumer-concurrency` |

Batch pattern: poll N rows → N async operations → `CompletableFuture.allOf()` → single `bulkUpdateRowStates`.

---

## Startup and refresh lifecycle

```mermaid
flowchart TD
  A[Spring Boot start] --> B[schema.sql]
  B --> C[VendorConfigurationResolver.refresh]
  C --> D[VendorCallbackPollingManager.start]
  D --> E[Schedule poll tasks]
  C --> F[VendorCallbackKafkaConsumerManager.start]
  G[DynamicVendorKafkaListenerManager.start]
  H[TelemetryConsumer registers]
  I[@Scheduled config-refresh-ms] --> C
  I --> J[rescheduleAll pollers]
  I --> K[kafkaConsumerManager.restart]
```

---

## Package dependency (callback module)

```mermaid
flowchart TB
  M[VendorCallbackPollingManager]
  M --> R[VendorConfigurationResolver]
  M --> Q[VendorSourceQueueJdbcRepository]
  M --> KT[VendorQueueKafkaPublishTask]
  M --> HT[VendorCallbackPollerTask]
  R --> JDB[VendorConfigurationJdbcRepository]
  R --> P[VendorCallbackSourceTableProvisioner]
  KT --> KP[VendorCallbackKafkaPublisher]
  KT --> PL[VendorPayloadConstructionService]
  KT --> ST[QueueRowStateTransitionService]
  KM[VendorCallbackKafkaConsumerManager] --> KC[VendorCallbackKafkaConsumer]
  KC --> D[VendorCallbackDispatcher]
  KC --> PL
  KC --> ST
  KC --> Q
```

---

## Seed data (local dev)

From `schema.sql`:

| vendor_name | queue_name | table_name | circle |
|-------------|------------|------------|--------|
| one97 | queue_callback_one97 | vendor_callback_queue_one97_tanzania | tanzania |
| paytmchemba | queue_callback_paytmchemba | vendor_callback_queue_paytmchemba | default |

Demo callback URL: `http://localhost:8080/actuator/health` (GET). Production deployments should point `callback_url` at real vendor endpoints.

---

## Design decisions

1. **`queue_name` vs `table_name`** — Kafka topic uses `queue_name`; all polling and state updates use `table_name`. They are linked by one `vendor_callback_queue_config` row.

2. **Separate ingest topics** — `{queue_name}.ingest` avoids mixing `VendorEvent` JSON with `VendorCallbackQueueMessage` on the same topic.

3. **JdbcTemplate over JPA** — Dynamic tables and column sets per vendor; metadata-driven INSERT and batch UPDATE.

4. **No `_producer` tables** — Removed legacy Kafka callback consumer that cloned rows into `*_producer` tables. State is tracked on the source queue row only.

5. **Config refresh** — Poller intervals and Kafka consumer containers can be rebound without full JVM restart (scheduled refresh).

---

## Extension points

| Change | Touch |
|--------|--------|
| New queue/vendor | DB rows in `vendor_callback_queue_config` + `sm_vendor_*` |
| New payload field | `sm_vendor_param_configuration` + column on source table |
| Custom HTTP headers | Extend `VendorCallbackDispatcher` |
| Metrics/tracing | Wrap dispatcher and publisher; add Micrometer |
| Disable Kafka for callbacks | `app.vendor-callback.dispatch-via-kafka: false` |

See also [PROJECT_STRUCTURE_DIAGRAM.md](PROJECT_STRUCTURE_DIAGRAM.md) for a concise package map.
