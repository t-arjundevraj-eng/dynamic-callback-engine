# Implementation Guide: SQL → Kafka → SQL Callback Queue Pipeline

## Overview

This implementation creates a **three-component pipeline** that reads from vendor-specific database tables, publishes to Kafka, and stores in producer tables.

### Flow Diagram

```
┌───────────���─────────────────────────────────────────────────────┐
│  STEP 1: DATABASE POLLING (DynamicPollingManager)               │
├────────────────────────���────────────────────────────────────────┤
│ Startup:
│  1. Read vendor_callback_queue_config table
│  2. Get all active configs (status = 1)
│  3. For each config: Schedule VendorQueuePollerTask
│
│ Example Config Row:
│ ┌────────┬──────────────────────┬─────────────┬─────────────────────┐
│ │ vendor │ circle               │ queue_name  │ table_name          │
│ ├────────┼──────────────────────┼─────────────┼─────────────────────┤
│ │ one97  │ tanzania             │ queue_...97 │ vendor_callback_... │
│ └────────┴──────────────────────┴─────────────┴─────────────────────┘
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│  STEP 2: SCHEDULED POLLING (VendorQueuePollerTask)              │
├─────────────────────────────────────────────────────────────────┤
│ Every producerSleepTime milliseconds (typically 1000ms):
│
│  1. Execute: SELECT * FROM vendor_callback_queue_one97_tanzania
│             LIMIT 50 (from config.fetchSize)
│
│  2. For each row (Map<String, Object>):
│     - Send entire row as JSON to Kafka
│     - Kafka topic: queue_callback_one97 (from config.queueName)
│     - Message key: queueId (for partitioning)
│
│  3. Log success/failure counts
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│  KAFKA BROKER (Message Hub)                                     │
├───────────────────────────────────��─────────────────────────────┤
│ Topics:
│  - queue_callback_one97
│  - queue_callback_paytmchemba
│  - queue_callback_contest
│  - queue_callback_contest_ivory
│  - vendor_callback_mptmyMyanmar
│  - queue_callback_mptmyANMAR
│  - queue_callback_one97_
│
│ Message Format: Full row from source table as JSON
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│  STEP 3: CONSUMER (CallbackEventConsumer)                       │
├─────────────────────────────────────────────────────────────────┤
│ Listens to all vendor queue topics
│
│ For each message received:
│  1. Extract Kafka topic name
│  2. Query VendorCallbackQueueConfigRepository.findActiveBuQueueName(topic)
│  3. Get source table name from config
│  4. Determine target table: <source_table>_producer
│     Example: vendor_callback_queue_one97_tanzania
│             → vendor_callback_queue_one97_tanzania_producer
│
│  5. Call CallbackEventJdbcRepository.save(config, row)
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────���───────────────────────────┐
│  STEP 4: REPOSITORY LAYER (CallbackEventJdbcRepository)         │
├─────────────────────────────────────────────────────────────────┤
│ 1. Ensure target table exists:
│    - If NOT exists: CREATE TABLE <target> LIKE <source>
│    - (Copies entire schema from source)
│
│ 2. Get metadata of target table:
│    - Query information_schema.columns
│    - Get all valid column names
│
│ 3. Filter incoming row to only matching columns
│
│ 4. Build dynamic INSERT:
│    INSERT INTO `vendor_callback_queue_one97_tanzania_producer`
│    (`col1`, `col2`, `col3`) VALUES (?, ?, ?)
│
│ 5. Execute with parameterized values
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│  STEP 5: TARGET TABLE STORAGE                                   │
├─────────────────────────────────────────────────────────────────┤
│ Database:
│  vendor_callback_queue_one97_tanzania_producer (WITH ROW DATA)
│  vendor_callback_queue_paytmchemba_producer (WITH ROW DATA)
│  vendor_callback_queue_contest_producer (WITH ROW DATA)
│  etc.
└─────────────────────────────────────────────────────────────────┘
```

---

## Component Details

### 1. DynamicPollingManager (@Service)

**Location:** `org.example.producer.DynamicPollingManager`

**Responsibilities:**
- Initializes on application startup via `@PostConstruct`
- Creates a `ThreadPoolTaskScheduler` with 20 threads
- Reads `vendor_callback_queue_config` table for all active configs
- Schedules a `VendorQueuePollerTask` for each config
- Uses `config.producerSleepTime` as the delay between polling cycles
- Gracefully shuts down via `@PreDestroy`

**Key Methods:**
```java
@PostConstruct
public void initializePolling()  // Startup initialization

@PreDestroy
public void shutdown()            // Graceful shutdown
```

---

### 2. VendorQueuePollerTask (implements Runnable)

**Location:** `org.example.producer.VendorQueuePollerTask`

**Responsibilities:**
- Executes at fixed intervals (scheduled by DynamicPollingManager)
- Polls the source table via JDBC query
- Sends each row as a Map to Kafka
- Exception-safe: catches all exceptions to prevent thread death

**Code Flow:**
```java
@Override
public void run() {
    try {
        pollAndPublish();
    } catch (Exception e) {
        log.error("...continue on next interval");
        // Does NOT rethrow - allows task to continue
    }
}

private void pollAndPublish() {
    // 1. Build safe table name
    String safeTableName = SqlIdentifier.tableName(config.getTableName());
    
    // 2. SELECT * FROM table LIMIT fetchSize
    List<Map<String, Object>> rows = jdbcTemplate.queryForList(selectQuery);
    
    // 3. For each row, send to Kafka
    for (Map<String, Object> row : rows) {
        kafkaTemplate.send(kafkaTopic, messageKey, row);
    }
}
```

---

### 3. CallbackEventConsumer (@Component)

**Location:** `org.example.consumer.CallbackEventConsumer`

**Responsibilities:**
- Listens to all vendor queue topics (defined in @KafkaListener)
- Receives each message as a Map
- Looks up the config by queue topic name
- Calls repository to save

**Key Code:**
```java
@KafkaListener(
    topics = "queue_callback_paytmchemba, queue_callback_one97, ...",
    groupId = "callback-event-consumer-group"
)
public void consume(
        @Payload Map<String, Object> records,
        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
    
    // Find config by topic
    VendorCallbackQueueConfig config = configRepository.findActiveByQueueName(topic);
    
    // Save to <table>_producer
    callbackRepository.save(config, records);
}
```

---

### 4. CallbackEventJdbcRepository (@Repository)

**Location:** `org.example.persistence.CallbackEventJdbcRepository`

**Responsibilities:**
- Dynamically creates target tables if they don't exist
- Validates incoming row columns against target table schema
- Builds and executes parameterized INSERT statements

**Key Methods:**

#### `save(config, row)`
```java
public void save(VendorCallbackQueueConfig config, Map<String, Object> row) {
    // 1. Determine target table name
    String targetTableName = config.getTableName() + "_producer";
    
    // 2. Ensure target table exists
    ensureTargetTableExists(sourceTableName, targetTableName);
    
    // 3. Get valid columns for target table
    Set<String> targetColumns = tableMetadataRepository.columns(targetTableName);
    
    // 4. Filter row to only valid columns
    List<String> insertColumns = row.keySet().stream()
        .filter(targetColumns::contains)
        .collect(Collectors.toList());
    
    // 5. Build INSERT INTO target (col1, col2) VALUES (?, ?)
    String sql = "INSERT INTO " + SqlIdentifier.tableName(targetTableName)
            + " (" + columns + ") VALUES (" + placeholders + ")";
    
    // 6. Execute with parameterized values
    jdbcTemplate.update(sql, values.toArray());
}
```

#### `ensureTargetTableExists(source, target)`
```java
private void ensureTargetTableExists(String sourceTableName, String targetTableName) {
    try {
        tableMetadataRepository.columns(targetTableName);
        // Table exists
    } catch (IllegalArgumentException e) {
        // Table doesn't exist, create it
        createTableFromSource(sourceTableName, targetTableName);
    }
}
```

#### `createTableFromSource(source, target)`
```java
private void createTableFromSource(String sourceTableName, String targetTableName) {
    String createTableSql = String.format(
        "CREATE TABLE IF NOT EXISTS %s LIKE %s",
        SqlIdentifier.tableName(targetTableName),
        SqlIdentifier.tableName(sourceTableName)
    );
    jdbcTemplate.execute(createTableSql);
}
```

---

## Database Schema Changes

### New Repository Method Required

**File:** `VendorCallbackQueueConfigRepository.java`

Add this method:
```java
public VendorCallbackQueueConfig findActiveByQueueName(String queueName) {
    List<VendorCallbackQueueConfig> configs = jdbcTemplate.query(
        SELECT_ACTIVE + " AND queue_name = ? LIMIT 1",
        new BeanPropertyRowMapper<>(VendorCallbackQueueConfig.class),
        queueName
    );
    return configs.isEmpty() ? null : configs.get(0);
}
```

---

## Example Execution Trace

### Startup
```
INFO: Initializing DynamicPollingManager
INFO: Found 7 active vendor queue configurations
INFO: Scheduling poller for vendor: one97, circle: tanzania, source table: vendor_callback_queue_one97_tanzania, sleep interval: 1000 ms
INFO: Scheduling poller for vendor: paytmchemba, circle: all, source table: vendor_callback_queue_paytmchemba_all, sleep interval: 1000 ms
INFO: DynamicPollingManager initialization complete with 7 polling tasks
```

### First Poll Cycle (1 second after startup)
```
DEBUG: Executing query for vendor one97, circle tanzania: 
       SELECT * FROM `vendor_callback_queue_one97_tanzania` LIMIT 50
INFO: Found 35 records for vendor: one97, circle: tanzania, table: vendor_callback_queue_one97_tanzania
INFO: Successfully published 35 of 35 rows to Kafka topic: queue_callback_one97 for vendor: one97, table: vendor_callback_queue_one97_tanzania
```

### Consumer Processing
```
DEBUG: Received callback event from topic: queue_callback_one97
DEBUG: Target table vendor_callback_queue_one97_tanzania_producer does not exist. Creating from source table schema...
INFO: Successfully created target table vendor_callback_queue_one97_tanzania_producer from source table vendor_callback_queue_one97_tanzania
DEBUG: Executing insert for table: vendor_callback_queue_one97_tanzania_producer with SQL: 
       INSERT INTO `vendor_callback_queue_one97_tanzania_producer` 
       (`col1`, `col2`, `col3`) VALUES (?, ?, ?)
DEBUG: Inserted 1 rows into table: vendor_callback_queue_one97_tanzania_producer
INFO: Stored callback event in table: vendor_callback_queue_one97_tanzania_producer for vendor: one97
```

---

## Configuration Required

### application.yml
```yaml
spring:
  kafka:
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
    consumer:
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      group-id: callback-event-consumer-group
      max-poll-records: 100
```

---

## Error Handling

### Polling Failures
- If a poll cycle throws exception → logged but doesn't stop scheduler
- Task continues running on next interval

### Consumer Failures
- If processing fails → logged but doesn't stop consumer
- Consumer continues listening for messages

### Table Creation Failures
- If target table can't be created → exception is thrown and logged
- Prevents data loss by failing fast on configuration issues

### SQL Injection Prevention
- All table names validated against regex: `^[A-Za-z0-9_]+$`
- All column names validated against same regex
- Table and column names wrapped in backticks
- All values passed as parameterized query parameters

---

## Performance Considerations

1. **Batch Size:** `fetch_size` from config (default 50)
   - Limits records polled per cycle

2. **Poll Interval:** `producer_sleep_time` from config (default 1000ms)
   - Configurable per vendor

3. **Thread Pool:** 20 threads in scheduler
   - Supports up to 20 concurrent vendor pollers

4. **Database:** Uses JDBC with connection pooling
   - Efficient batch operations

5. **Kafka:** Uses `KafkaTemplate.send()` with async callbacks
   - Non-blocking publish

---

## Testing Checklist

- [ ] 1. Application starts without errors
- [ ] 2. DynamicPollingManager reads vendor_callback_queue_config
- [ ] 3. Scheduler has 7 tasks (one per config)
- [ ] 4. Source tables have data to poll
- [ ] 5. First poll cycle fetches data and publishes to Kafka
- [ ] 6. Kafka consumer receives messages
- [ ] 7. Target _producer tables are created automatically
- [ ] 8. Data is inserted into target tables
- [ ] 9. Verify row count: source table rows → Kafka → target table
- [ ] 10. No data loss or duplication

---

## Troubleshooting

| Symptom | Cause | Solution |
|---------|-------|----------|
| Polling doesn't start | Missing @Service on DynamicPollingManager | Verify @Service annotation exists |
| No Kafka topics created | KafkaConfig not creating topics | Check KafkaConfig bean |
| Consumer not processing | Topics not in @KafkaListener | Add all topic names |
| Target table not created | Source table doesn't match schema | Verify source table exists |
| SQL injection errors | Invalid table names in config | Ensure only alphanumeric + underscore |
| Duplicate data | JDBC not using INSERT properly | Check parameterized query usage |

---

## Files Modified/Created

```
CREATED:
✓ org.example.producer.DynamicPollingManager
✓ org.example.producer.VendorQueuePollerTask  
✓ org.example.consumer.CallbackEventConsumer
✓ org.example.persistence.CallbackEventJdbcRepository

MODIFIED:
✓ org.example.persistence.VendorCallbackQueueConfigRepository
  (Added: findActiveByQueueName() method)

UNCHANGED (Already exist):
✓ org.example.persistence.VendorTableMetadataRepository
✓ org.example.persistence.VendorCallbackQueueConfig
✓ org.example.persistence.SqlIdentifier
```


