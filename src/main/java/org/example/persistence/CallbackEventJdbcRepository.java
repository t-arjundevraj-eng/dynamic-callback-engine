package org.example.persistence;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Repository for storing callback queue records in vendor-specific producer tables.
 * Dynamically inserts records into <table_name>_producer tables based on configuration.
 */
@Repository
@Slf4j
public class CallbackEventJdbcRepository {

    private final JdbcTemplate jdbcTemplate;
    private final VendorTableMetadataRepository tableMetadataRepository;

    public CallbackEventJdbcRepository(
            JdbcTemplate jdbcTemplate,
            VendorTableMetadataRepository tableMetadataRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.tableMetadataRepository = tableMetadataRepository;
    }

    /**
     * Inserts a callback event record into the producer table.
     * The target table is named as: <source_table_name>_producer
     *
     * For example, if source table is "vendor_callback_queue_one97_tanzania",
     * it will insert into "vendor_callback_queue_one97_tanzania_producer".
     *
     * @param config The vendor queue configuration containing source table name
     * @param row    The data row from Kafka (Map of column names to values)
     */
    public void save(VendorCallbackQueueConfig config, Map<String, Object> row) {
        if (row == null || row.isEmpty()) {
            log.warn("Received empty row for config: {}. Skipping insert.", config.getTableName());
            return;
        }

        // Build target table name: source_table_producer
        String sourceTableName = config.getTableName();
        String targetTableName = sourceTableName + "_producer";

        // Ensure target table exists with same schema as source
        ensureTargetTableExists(sourceTableName, targetTableName);

        // Get metadata of target table to know valid columns
        java.util.Set<String> targetColumns = tableMetadataRepository.columns(targetTableName);

        // Filter incoming row to only include columns that exist in target table
        List<String> insertColumns = row.keySet().stream()
                .filter(targetColumns::contains)
                .collect(Collectors.toList());

        if (insertColumns.isEmpty()) {
            log.error("No columns from incoming row match target table {}. Row keys: {}, Table columns: {}",
                    targetTableName, row.keySet(), targetColumns);
            return;
        }

        // Build dynamic INSERT statement
        String columns = insertColumns.stream()
                .map(SqlIdentifier::columnName)
                .collect(Collectors.joining(", "));

        String placeholders = insertColumns.stream()
                .map(col -> "?")
                .collect(Collectors.joining(", "));

        String sql = "INSERT INTO " + SqlIdentifier.tableName(targetTableName)
                + " (" + columns + ") VALUES (" + placeholders + ")";

        log.debug("Executing insert for table: {} with SQL: {}", targetTableName, sql);

        // Gather values in order
        List<Object> values = new ArrayList<>();
        for (String column : insertColumns) {
            values.add(row.get(column));
        }

        // Execute insert
        try {
            int rowsInserted = jdbcTemplate.update(sql, values.toArray());
            log.debug("Inserted {} rows into table: {}", rowsInserted, targetTableName);
        } catch (Exception e) {
            log.error("Failed to insert record into table: {}", targetTableName, e);
            throw e;
        }
    }

    /**
     * Ensures that the target producer table exists with the same schema as the source table.
     * If the target table doesn't exist, it creates it from the source table schema.
     *
     * @param sourceTableName The original source table name
     * @param targetTableName The target producer table name
     */
    private void ensureTargetTableExists(String sourceTableName, String targetTableName) {
        try {
            // Try to get columns - if table exists, this will succeed
            tableMetadataRepository.columns(targetTableName);
            log.debug("Target table {} already exists", targetTableName);
        } catch (IllegalArgumentException e) {
            // Table doesn't exist, create it from source schema
            log.info("Target table {} does not exist. Creating from source table schema...", targetTableName);
            createTableFromSource(sourceTableName, targetTableName);
        }
    }

    /**
     * Creates target producer table with the same schema as the source table.
     *
     * @param sourceTableName The source table to copy schema from
     * @param targetTableName The target table to create
     */
    private void createTableFromSource(String sourceTableName, String targetTableName) {
        try {
            String createTableSql = String.format(
                    "CREATE TABLE IF NOT EXISTS %s LIKE %s",
                    SqlIdentifier.tableName(targetTableName),
                    SqlIdentifier.tableName(sourceTableName)
            );

            log.debug("Creating target table with SQL: {}", createTableSql);
            jdbcTemplate.execute(createTableSql);
            log.info("Successfully created target table {} from source table {}", targetTableName, sourceTableName);
        } catch (Exception e) {
            log.error("Failed to create target table {} from source table {}", targetTableName, sourceTableName, e);
            throw e;
        }
    }
}

