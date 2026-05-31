# Callback Queue Pipeline - Quick Reference

## What Was Created

This implementation transforms your application into a **SQL → Kafka → SQL pipeline** for vendor callbacks.

### Files Created (4 new Java classes + 1 helper method)

```
1. DynamicPollingManager.java
   Location: src/main/java/org/example/producer/
   Type: @Service
   Purpose: Initializes polling on startup, schedules tasks

2. VendorQueuePollerTask.java
   Location: src/main/java/org/example/producer/
   Type: Runnable
   Purpose: Executes polling for one vendor config

3. CallbackEventConsumer.java
   Location: src/main/java/org/example/consumer/
   Type: @Component
   Purpose: Listens to Kafka, receives events

4. CallbackEventJdbcRepository.java
   Location: src/main/java/org/example/persistence/
   Type: @Repository
   Purpose: Stores events in <table>_producer tables

5. VendorCallbackQueueConfigRepository.java (MODIFIED)
   Added Method: findActiveByQueueName(String queueName)
```

---

## Complete Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│ STARTUP                                                              │
├─────────────────────────────────────────────────────────────────────┤
│
│  Application Starts
│  ↓
│  DynamicPollingManager.@PostConstruct
│  ↓
│  ThreadPoolTaskScheduler created (20 threads)
│  ↓  
│  SELECT * FROM vendor_callback_queue_config WHERE status = 1
│  ↓
│  Creates VendorQueuePollerTask for each config:
│    - vendor: one97, circle: tanzania
│    - vendor: paytmchemba, circle: all
│    - vendor: contest, circle: all
│    - vendor: contest, circle: ivory
│    - vendor: mptvendor, circle: all
│    - vendor: mptvendor, circle: mya
│    - vendor: one97, circle: tanzania (alternate)
│  ↓
│  Schedules each task with producerSleepTime delay (typically 1000ms)
│
└─────────────────────────────────────────────────────────────────────┘

        ↓ Every 1000ms for each vendor

┌─────────────────────────────────────────────────────────────────────┐
│ POLLING (VendorQueuePollerTask.run())                               │
├─────────────────────────────────────────────────────────────────────┤
│
│  FOR vendor = one97, table = vendor_callback_queue_one97_tanzania
│  ↓
│  Execute:
│    SELECT * FROM `vendor_callback_queue_one97_tanzania` LIMIT 50
│  ↓
│  Returns List<Map<String, Object>>
│    [
│      {col1: val1, col2: val2, col3: val3},
│      {col1: val1, col2: val2, col3: val3},
│      ...
│    ]
│  ↓
│  FOR each row:
│    ├─ kafkaTemplate.send(
│    │    topic: "queue_callback_one97",
│    │    key: "15",  // queueId
│    │    value: {col1: val1, col2: val2, col3: val3}
│    │  )
│    └─ LOG success
│  ↓
│  Log: "Published 50 messages to queue_callback_one97"
│
└─────────────────────────────────────────────────────────────────────┘

        ↓ Async message delivery

┌─────────────────────────────────────────────────────────────────────┐
│ KAFKA BROKER (Distributed Message Queue)                            │
├─────────────────────────────────────────────────────────────────────┤
│
│ Topics:
│   queue_callback_one97 → partition 0, 1, 2, ...
│   queue_callback_paytmchemba → partition 0, 1, 2, ...
│   queue_callback_contest → partition 0, 1, 2, ...
│   ... (7 topics total)
│
│ Each message:
│   {
│     "key": "15",
│     "value": {
│       "col1": "value1",
│       "col2": "value2",
│       "col3": "value3"
│     },
│     "timestamp": 1234567890,
│     "partition": 0,
│     "offset": 999
│   }
│
└─────────────────────────────────────────────────────────────────────┘

        ↓ Consumer group polls

┌─────────────────────────────────────────────────────────────────────┐
│ CONSUMING (CallbackEventConsumer.consume())                         │
├─────────────────────────────────────────────────────────────────────┤
│
│  @KafkaListener(
│    topics = "queue_callback_one97, ...",
│    groupId = "callback-event-consumer-group"
│  )
│  ↓
│  Receives message:
│    topic: "queue_callback_one97"
│    payload: {col1: val1, col2: val2, col3: val3}
│  ↓
│  VendorCallbackQueueConfigRepository.findActiveByQueueName("queue_callback_one97")
│  ↓
│  Returns VendorCallbackQueueConfig:
│    {
│      vendor: "one97",
│      circle: "tanzania",
│      tableName: "vendor_callback_queue_one97_tanzania"
│    }
│  ↓
│  CallbackEventJdbcRepository.save(config, row)
│
└─────────────────────────────────────────────────────────────────────┘

        ↓

┌─────────────────────────────────────────────────────────────────────┐
│ PERSISTENCE (CallbackEventJdbcRepository.save())                    │
├─────────────────────────────────────────────────────────────────────┤
│
│  1. targetTableName = "vendor_callback_queue_one97_tanzania_producer"
│  
│  2. Check if target table exists:
│     SELECT COLUMN_NAME FROM information_schema.columns
│     WHERE TABLE_NAME = "vendor_callback_queue_one97_tanzania_producer"
│     ↓
│     NOT FOUND? → CREATE TABLE
│  
│  3. CREATE TABLE IF NOT EXISTS
│     `vendor_callback_queue_one97_tanzania_producer`
│     LIKE `vendor_callback_queue_one97_tanzania`
│  
│  4. Get valid columns from target table
│     targetColumns = {col1, col2, col3, col4, ...}
│  
│  5. Filter incoming row to matching columns
│     insertColumns = {col1, col2, col3}  (row columns)
│  
│  6. Build parameterized INSERT:
│     INSERT INTO `vendor_callback_queue_one97_tanzania_producer`
│     (`col1`, `col2`, `col3`)
│     VALUES (?, ?, ?)
│  
│  7. Execute with values:
│     [val1, val2, val3]
│  
│  8. Result: 1 row inserted
│     ✓ vendor_callback_queue_one97_tanzania_producer
│
└─────────────────────────────────────────────────────────────────────┘

        ↓ Repeats for each message

┌─────────────────────────────────────────────────────────────────────┐
│ TARGET TABLES (Storage)                                              │
├─────────────────────────────────────────────────────────────────────┤
│
│ MySQL Database:
│
│   vendor_callback_queue_one97_tanzania_producer
│   ├─ Row 1: {col1: val1, col2: val2, col3: val3}
│   ├─ Row 2: {col1: val1, col2: val2, col3: val3}
│   └─ Row N: {col1: val1, col2: val2, col3: val3}
│
│   vendor_callback_queue_paytmchemba_all_producer
│   ├─ Row 1: {col1: val1, col2: val2, col3: val3}
│   └─ Row M: {col1: val1, col2: val2, col3: val3}
│
│   ... (7 tables total, one per vendor queue config)
│
└─────────────────────────────────────────────────────────────────────┘
```

---

## Key Characteristics

### Input (Source)
- **What:** Vendor callback queue tables
- **Where:** Database (7 tables from vendor_callback_queue_config)
- **How:** Polled on a schedule
- **Example:** `vendor_callback_queue_one97_tanzania`

### Processing
- **Polling:** Every `producerSleepTime` ms (default 1000ms)
- **Batch Size:** `fetchSize` rows at a time (default 50)
- **Parallelism:** 20 concurrent pollers via ThreadPoolTaskScheduler
- **Exception Handling:** Failures don't stop polling

### Transport
- **Method:** Kafka topics
- **Format:** Full row as JSON Map<String, Object>
- **Key:** Queue ID (for partitioning)
- **Topics:** 7 Kafka topics matching queue_name configs

### Output (Target)
- **What:** Producer tables (<source>_producer suffix)
- **Where:** Same database
- **How:** Auto-created if missing (schema copied from source)
- **Example:** `vendor_callback_queue_one97_tanzania_producer`

---

## Configuration Details

### vendor_callback_queue_config Table

Used at runtime to configure polling:

```
queue_name                     → Kafka topic name (listener subscribes to this)
table_name                     → Source table to poll from
producer_sleep_time            → Polling interval (milliseconds)
fetch_size                     → Batch size per poll
vendor_name + circle_name      → Vendor identification
max_retry_count                → Not used in this implementation (for future)
```

### KafkaTemplate Configuration

The application uses:
```java
KafkaTemplate<String, Object>  // Key: String, Value: Object (Map)
```

Spring's `JsonSerializer` automatically converts Map → JSON

---

## Database Operations

### Read (SELECT)
```sql
-- Polling query (executed every producer_sleep_time)
SELECT * FROM `vendor_callback_queue_one97_tanzania` LIMIT 50
```

### Write (INSERT)
```sql
-- Dynamic INSERT (columns filtered to match target table)
INSERT INTO `vendor_callback_queue_one97_tanzania_producer`
(`col1`, `col2`, `col3`) VALUES (?, ?, ?)
```

### Schema Sync (CREATE)
```sql
-- Automatic target table creation
CREATE TABLE IF NOT EXISTS `vendor_callback_queue_one97_tanzania_producer`
LIKE `vendor_callback_queue_one97_tanzania`
```

---

## Thread Model

### Startup (Main Thread)
```
DynamicPollingManager.@PostConstruct
  ↓
Creates ThreadPoolTaskScheduler (20 core threads)
  ↓
Spawns 7 polling threads (one per vendor config)
  ↓
Each thread runs VendorQueuePollerTask.run() on schedule
```

### Runtime (Polling Threads)
```
Thread-1: Polls vendor_callback_queue_one97_tanzania every 1000ms
Thread-2: Polls vendor_callback_queue_paytmchemba_all every 1000ms
Thread-3: Polls vendor_callback_queue_contest every 1000ms
... (7 threads total)
```

### Consumer (Kafka Listener Thread)
```
kafka-consumer-thread: Listens to all 7 topics
  ↓
Receives message from any topic
  ↓
Calls CallbackEventConsumer.consume()
  ↓
Stores in corresponding _producer table
```

---

## SQL Injection Prevention

All dynamic SQL uses:

1. **Identifier Validation**
   ```java
   SqlIdentifier.tableName(name)   // Regex: ^[A-Za-z0-9_]+$
   SqlIdentifier.columnName(name)  // Regex: ^[A-Za-z0-9_]+$
   ```
   - Throws IllegalArgumentException if unsafe

2. **Backtick Wrapping**
   ```sql
   `vendor_callback_queue_one97_tanzania`  ← Safe
   `col1`                                   ← Safe
   ```

3. **Parameterized Queries**
   ```java
   jdbcTemplate.update(sql, values.toArray())
   // Values passed separately from SQL
   ```

---

## Error Scenarios & Recovery

### Scenario 1: Poll Fails (Network/DB timeout)
```
VendorQueuePollerTask.run()
  → Exception caught
  → Logged (error level)
  → NOT rethrown
  → Task scheduled again in 1000ms
  ✓ Resilient, continues polling
```

### Scenario 2: Kafka Publish Fails
```
kafkaTemplate.send(topic, key, value)
  → Exception caught
  → Logged (error level)
  → Loop continues to next row
  ✓ Doesn't block entire batch
```

### Scenario 3: Consumer Processing Fails
```
CallbackEventConsumer.consume()
  → Exception caught
  → Logged (error level)
  → NOT rethrown
  → Listener continues receiving
  ✓ Doesn't stop consumer
```

### Scenario 4: Target Table Creation Fails
```
CallbackEventJdbcRepository.save()
  → Table creation fails
  → Exception thrown/logged
  → Consumer handler catches
  → Consumer continues (data lost)
  ⚠ Should investigate DB permissions
```

---

## Monitoring & Logging

### Key Log Statements

**Startup:**
```
INFO: Found 7 active vendor queue configurations
INFO: Scheduling poller for vendor: one97, circle: tanzania...
INFO: DynamicPollingManager initialization complete with 7 polling tasks
```

**Polling:**
```
DEBUG: Executing query for vendor one97: SELECT * FROM...
INFO: Found 50 records for vendor: one97...
INFO: Successfully published 50 of 50 rows to Kafka topic: queue_callback_one97...
```

**Consuming:**
```
DEBUG: Received callback event from topic: queue_callback_one97
INFO: Successfully created target table vendor_callback_queue_one97_tanzania_producer...
DEBUG: Inserted 1 rows into table: vendor_callback_queue_one97_tanzania_producer
INFO: Stored callback event in table: vendor_callback_queue_one97_tanzania_producer
```

---

## Integration Checklist

- [ ] Files created in correct packages
- [ ] VendorCallbackQueueConfigRepository.findActiveByQueueName() added
- [ ] No import errors or missing dependencies
- [ ] Project compiles successfully
- [ ] Application can start without errors
- [ ] All 7 vendor configs are loaded
- [ ] 7 polling threads are scheduled
- [ ] First poll cycle reads data from source table
- [ ] Data published to Kafka successfully
- [ ] Consumer receives messages
- [ ] Target _producer tables are auto-created
- [ ] Data inserted into target tables
- [ ] No duplication or data loss


