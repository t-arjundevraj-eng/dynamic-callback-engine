# Technical Reference - Code Patterns & Flow

## 1. Startup Sequence

### Execution Order
```
1. Spring Boot Starts
   ↓
2. All @Service, @Component, @Repository beans created
   ↓
3. DynamicPollingManager constructor called
   ├─ Injects VendorCallbackQueueConfigRepository
   ├─ Injects JdbcTemplate
   └─ Injects KafkaTemplate<String, Object>
   ↓
4. DynamicPollingManager.@PostConstruct executes
   ├─ Creates ThreadPoolTaskScheduler(poolSize=20)
   ├─ Queries vendor_callback_queue_config table
   ├─ Gets List<VendorCallbackQueueConfig> (7 items)
   ├─ For each config:
   │  ├─ Creates new VendorQueuePollerTask(config, jdbc, kafka)
   │  ├─ Calls taskScheduler.scheduleWithFixedDelay(task, sleepTime)
   │  └─ Logs message
   └─ Logs "initialization complete"
   ↓
5. CallbackEventConsumer bean created
   ├─ Injects CallbackEventJdbcRepository
   ├─ Injects VendorCallbackQueueConfigRepository
   └─ @KafkaListener subscribes to all 7 topics
   ↓
6. Application Ready
   ↓
7. First polling cycle starts (~1000ms later)
```

---

## 2. Polling Task Execution (Every 1000ms)

### VendorQueuePollerTask.run() Call Stack

```
Scheduler thread-1 @ 1000ms
  ↓
  taskScheduler.scheduleWithFixedDelay(pollerTask, 1000) triggers
  ↓
  pollerTask.run() called
    ├─ try {
    │   ├─ pollAndPublish()
    │   │   ├─ SqlIdentifier.tableName(config.getTableName())
    │   │   │   └─ Returns: "`vendor_callback_queue_one97_tanzania`"
    │   │   │
    │   │   ├─ Build query:
    │   │   │   "SELECT * FROM `vendor_callback_queue_one97_tanzania` LIMIT 50"
    │   │   │
    │   │   ├─ jdbcTemplate.queryForList(query)
    │   │   │   ├─ Executes SELECT
    │   │   │   ├─ Parses ResultSet
    │   │   │   └─ Returns List<Map<String, Object>>
    │   │   │       = [
    │   │   │          {col1: val1, col2: val2, col3: val3},
    │   │   │          {col1: val1, col2: val2, col3: val3},
    │   │   │          ... (50 rows)
    │   │   │         ]
    │   │   │
    │   │   ├─ For each row (50 iterations):
    │   │   │   ├─ kafkaTemplate.send(
    │   │   │   │    topic: "queue_callback_one97",
    │   │   │   │    key: "17",
    │   │   │   │    value: {col1: val1, col2: val2, col3: val3}
    │   │   │   │  )
    │   │   │   ├─ Returns ListenableFuture
    │   │   │   └─ Logs "Published"
    │   │   │
    │   │   └─ Log success: "Published 50 of 50 rows"
    │   └─
    │  } catch (Exception e) {
    │     ├─ log.error("Error in polling...", e)
    │     └─ NOT rethrown (important!)
    │  }
    │
    └─ Thread continues, no exception thrown
    ↓
  threadScheduler continues running (resilient)
  ↓
  Next poll cycle @ 2000ms (1000ms delay)
```

---

## 3. Kafka Message Flow

### Message Creation in VendorQueuePollerTask

```java
Map<String, Object> row = {
  "id": 1234,
  "customer_id": "CUST-001",
  "phone": "9876543210",
  "amount": 100.50,
  "status": "PENDING",
  "created_at": "2026-05-28 12:34:56"
};

kafkaTemplate.send(
  "queue_callback_one97",  // Topic
  "17",                     // Key (queueId)
  row                       // Value (Map)
);
```

### Kafka Broker Receives

```
Kafka Topic: queue_callback_one97
Partition: 0 (based on key hash)
Message:
  ├─ key: "17"
  ├─ value: {
  │    "id": 1234,
  │    "customer_id": "CUST-001",
  │    "phone": "9876543210",
  │    "amount": 100.50,
  │    "status": "PENDING",
  │    "created_at": "2026-05-28 12:34:56"
  │  }
  │  (JSON serialized by Spring's JsonSerializer)
  ├─ timestamp: 1234567890123
  ├─ offset: 999
  └─ partition: 0
```

---

## 4. Consumer Processing

### CallbackEventConsumer.consume() Execution

```
Kafka Consumer Thread
  ↓
  Message arrives on topic "queue_callback_one97"
  ↓
  Consumer polls (max.poll.records=100)
  ↓
  CallbackEventConsumer.consume() called with:
    ├─ @Payload records = {
    │    "id": 1234,
    │    "customer_id": "CUST-001",
    │    "phone": "9876543210",
    │    "amount": 100.50,
    │    "status": "PENDING",
    │    "created_at": "2026-05-28 12:34:56"
    │  }
    │  (Auto-deserialized by Spring)
    │
    └─ @Header topic = "queue_callback_one97"
  ↓
  try {
    ├─ VendorCallbackQueueConfigRepository.findActiveByQueueName(
    │    "queue_callback_one97"
    │  )
    │  └─ Returns VendorCallbackQueueConfig:
    │     {
    │       queueId: 17,
    │       queueName: "queue_callback_one97",
    │       tableName: "vendor_callback_queue_one97_tanzania",
    │       vendorName: "one97",
    │       circleName: "tanzania"
    │     }
    │
    ├─ CallbackEventJdbcRepository.save(config, records)
    │  └─ (See next section)
    │
    └─ Log success
  } catch (Exception e) {
    ├─ log.error("Error...", e)
    └─ NOT rethrown (continues listening)
  }
```

---

## 5. Repository Save Operation

### CallbackEventJdbcRepository.save() Execution

```
save(config, records) called
  ↓
  1. Determine target table
     ├─ sourceTable = config.getTableName()
     │              = "vendor_callback_queue_one97_tanzania"
     └─ targetTable = sourceTable + "_producer"
                    = "vendor_callback_queue_one97_tanzania_producer"
  ↓
  2. Ensure target table exists
     ├─ ensureTargetTableExists(
     │    "vendor_callback_queue_one97_tanzania",
     │    "vendor_callback_queue_one97_tanzania_producer"
     │  )
     │
     ├─ try {
     │    tableMetadataRepository.columns(
     │      "vendor_callback_queue_one97_tanzania_producer"
     │    )
     │    └─ Queries DatabaseMetaData
     │       └─ Returns Set<String> of columns
     │
     │  } catch (IllegalArgumentException e) {
     │    // Table doesn't exist
     │
     │    createTableFromSource(
     │      "vendor_callback_queue_one97_tanzania",
     │      "vendor_callback_queue_one97_tanzania_producer"
     │    )
     │    ├─ Builds SQL:
     │    │  "CREATE TABLE IF NOT EXISTS
     │    │   `vendor_callback_queue_one97_tanzania_producer`
     │    │   LIKE `vendor_callback_queue_one97_tanzania`"
     │    │
     │    └─ jdbcTemplate.execute(sql)
     │       └─ Creates table with identical schema
     │  }
  ↓
  3. Get valid columns from target table
     ├─ Set<String> targetColumns = 
     │    tableMetadataRepository.columns(
     │      "vendor_callback_queue_one97_tanzania_producer"
     │    )
     │  = {id, customer_id, phone, amount, status, created_at, ...}
  ↓
  4. Filter incoming row to valid columns
     ├─ List<String> insertColumns = records.keySet().stream()
     │    .filter(targetColumns::contains)
     │    .collect(Collectors.toList())
     │
     └─ = {id, customer_id, phone, amount, status, created_at}
        (Any extra columns discarded)
  ↓
  5. Build parameterized INSERT statement
     ├─ columns = "`id`, `customer_id`, `phone`, `amount`, `status`, `created_at`"
     ├─ placeholders = "?, ?, ?, ?, ?, ?"
     │
     └─ sql = "INSERT INTO `vendor_callback_queue_one97_tanzania_producer`"
              "(`id`, `customer_id`, `phone`, `amount`, `status`, `created_at`)"
              "VALUES (?, ?, ?, ?, ?, ?)"
  ↓
  6. Gather values in order
     ├─ values = [1234, "CUST-001", "9876543210", 100.50, "PENDING", "2026-05-28 12:34:56"]
  ↓
  7. Execute parameterized update
     ├─ jdbcTemplate.update(sql, values.toArray())
     │  ├─ Compiles prepared statement
     │  ├─ Binds parameters safely
     │  │  └─ value[0] → ?
     │  │  └─ value[1] → ?
     │  │  └─ ... etc
     │  ├─ Executes
     │  └─ Returns 1 (rows affected)
  ↓
  8. Log success
     └─ "Inserted 1 rows into table: vendor_callback_queue_one97_tanzania_producer"
```

---

## 6. SQL Injection Prevention in Detail

### Table Name Validation

```java
String tableName = "vendor_callback_queue_one97_tanzania";

SqlIdentifier.tableName(tableName)
  ├─ Check regex: ^[A-Za-z0-9_]+$
  ├─ tableName.matches(IDENTIFIER_PATTERN)
  │  └─ true ✅
  ├─ Return: "`" + tableName + "`"
  └─ Result: "`vendor_callback_queue_one97_tanzania`"

// Attack attempt
String malicious = "vendor_table; DROP TABLE users; --";

SqlIdentifier.tableName(malicious)
  ├─ Check regex: ^[A-Za-z0-9_]+$
  ├─ malicious.matches(IDENTIFIER_PATTERN)
  │  └─ false ❌ (contains semicolon and space)
  └─ throw new IllegalArgumentException("Unsafe table name: ...")
```

### Parameterized Query Protection

```java
// UNSAFE (vulnerable to SQL injection):
String unsafe = "INSERT INTO table (col) VALUES ('" + userInput + "')";
// If userInput = "'; DROP TABLE users; --"
// Result: INSERT INTO table (col) VALUES (''; DROP TABLE users; --')
//         → Could inject SQL

// SAFE (parameterized):
String safe = "INSERT INTO table (col) VALUES (?)";
jdbcTemplate.update(safe, userInput);
// userInput value passed separately
// JDBC driver escapes special characters
// Result: Treated as literal string, not SQL
```

---

## 7. Thread Safety & Concurrency

### ThreadPoolTaskScheduler Diagram

```
Spring Boot Application
  ↓
DynamicPollingManager created
  ↓
ThreadPoolTaskScheduler(poolSize=20)
  ├─ Main scheduler thread
  ├─ Thread pool with 20 worker threads
  │  ├─ Thread-1 → Polling schedule 1
  │  ├─ Thread-2 → Polling schedule 2
  │  ├─ Thread-3 → Polling schedule 3
  │  ├─ Thread-4 → Polling schedule 4
  │  ├─ Thread-5 → Polling schedule 5
  │  ├─ Thread-6 → Polling schedule 6
  │  ├─ Thread-7 → Polling schedule 7
  │  ├─ Thread-8 → (available for other tasks)
  │  └─ ... Thread-20 → (available for other tasks)
  └└─┘

All threads:
✅ Thread-safe JDBC (connection pooling)
✅ Thread-safe Kafka client
✅ Spring beans are singleton (thread-safe)
✅ No shared mutable state (each task is independent)
```

### Consumer Thread

```
Spring Context
  ├─ Kafka Consumer Listener Container (runs in background)
  │  └─ kafka-consumer-thread-1
  │     ├─ Polls Kafka every 300ms (default)
  │     ├─ Receives batch of messages
  │     └─ Calls CallbackEventConsumer.consume() for each
  │
  └─ CallbackEventConsumer
     ├─ Listener thread (concurrent=1)
     └─ Processes one message at a time
```

---

## 8. Exception Flow

### Resilience Pattern 1: Polling Task

```
VendorQueuePollerTask.run()
  ├─ try {
  │   └─ pollAndPublish()
  │       ├─ Database connection fails
  │       │  └─ throw SQLException
  │       │     ↓
         │     (continues in catch)
  │       │
  │       ├─ Kafka broker unavailable
  │       │  └─ throw KafkaException
  │       │     ↓
  │       │     (continues in catch)
  │  }
  │
  ├─ catch (Exception e) {
  │   ├─ log.error("Error in polling...", e)
  │   ├─ // NO rethrow
  │   └─ // Method returns normally
  │
  └─ }
  (next poll cycle @ +1000ms)
```

### Resilience Pattern 2: Consumer

```
CallbackEventConsumer.consume()
  ├─ try {
  │   ├─ VendorCallbackQueueConfigRepository.findActiveByQueueName()
  │   │  └─ returns null (config not found)
  │   │  └─ log.warn("No config found")
  │   │  └─ return (early exit, safe)
  │   │
  │   ├─ CallbackEventJdbcRepository.save()
  │   │  ├─ Database connection fails
  │   │  │  └─ throw SQLException
  │   │  │     ↓
  │   │  │     (continues in catch)
  │   │  │
  │   │  └─ Table creation fails
  │   │     └─ throw Exception
  │   │        ↓
  │   │        (continues in catch)
  │  }
  │
  ├─ catch (Exception e) {
  │   ├─ log.error("Error processing...", e)
  │   ├─ // NO rethrow
  │   └─ // Listener continues running
  │
  └─ }
  (next message pulled from Kafka)
```

---

## 9. Complete End-to-End Timing

### 5-Second Application Lifecycle

```
Time: 0ms
├─ Application starts
├─ Beans created
└─ DynamicPollingManager.@PostConstruct runs
   ├─ ThreadPoolTaskScheduler created
   ├─ 7 polling tasks scheduled
   └─ First tasks queued to start at ~1000ms

Time: 500ms
└─ CallbackEventConsumer listening to all 7 topics
   └─ No messages yet

Time: 1000ms
├─ VendorQueuePollerTask-1 executes
│  ├─ Polls vendor_callback_queue_one97_tanzania
│  ├─ Gets 50 rows
│  └─ Sends 50 messages to Kafka topic
├─ VendorQueuePollerTask-2 executes (paytmchemba)
├─ ... all 7 tasks execute concurrently
└─ Total: 350 messages sent to Kafka (7 tasks × 50 rows)

Time: 1050ms
├─ Kafka broker receives 350 messages
├─ Consumer begins pulling from topics
├─ CallbackEventConsumer.consume() called 350 times
├─ 350 rows inserted into _producer tables
└─ Logs: "Stored callback event..." (350 times)

Time: 2000ms
├─ Second polling cycle starts
├─ 350 more messages sent to Kafka
└─ 350 more rows to be consumed

Time: 3000ms
├─ Third polling cycle
└─ Consumer still processing cycle 2 messages

Time: 4000ms
├─ Fourth polling cycle
└─ Consumer still catching up

Time: 5000ms
├─ Fifth polling cycle
└─ Target _producer tables now have 1750+ rows
   (5 cycles × 350 messages per cycle)
```

---

## Summary Table: Data Transformations

```
┌────────────────────────────────────────────────────────────���─┐
│ SOURCE                    │ FORMAT    │ TARGET               │
├──────────────────────────────────────────────────────────────┤
│ vendor_callback_queue     │ SQL       │ List<Map>            │
│ _one97_tanzania           │ Table     │                      │
│                           │           │                      │
│ List<Map>                 │ Java Obj  │ Kafka Msg (AVRO/JSON)│
│ 50 rows                   │           │ 50 messages          │
│                           │           │                      │
│ Kafka Message             │ JSON      │ Map<String, Object>  │
│ (30 MB/sec throughput)    │ Wire      │ (deserialized)       │
│                           │           │                      │
│ Map<String, Object>       │ Java Obj  │ SQL INSERT           │
│ (50 records/batch)        │           │ (INSERT)             │
│                           │           │                      │
│ INSERT Statement          │ SQL       │ DB Row               │
│ (parameterized)           │ Wire      │ vendor_callback_     │
│                           │           │ _one97_tanzania_     │
│                           │           │ producer             │
└──────────────────────────────────────────────────────────────┘
```

---

This technical reference shows exactly how every component interacts and processes data through the pipeline.


