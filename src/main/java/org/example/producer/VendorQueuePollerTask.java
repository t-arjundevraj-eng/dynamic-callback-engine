package org.example.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.example.persistence.SqlIdentifier;
import org.example.persistence.VendorCallbackQueueConfig;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Polls a specific vendor database table at configured intervals,
 * sends all rows to Kafka topic, and optionally marks rows as processed.
 * Designed to run as a scheduled task with exception handling to ensure
 * the polling thread continues running despite transient failures.
 *
 * Flow: Source Table → Kafka → Consumer stores in <table>_producer
 */
@Slf4j
public class VendorQueuePollerTask implements Runnable {

    private final VendorCallbackQueueConfig config;
    private final JdbcTemplate jdbcTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public VendorQueuePollerTask(
            VendorCallbackQueueConfig config,
            JdbcTemplate jdbcTemplate,
            KafkaTemplate<String, Object> kafkaTemplate) {
        this.config = config;
        this.jdbcTemplate = jdbcTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void run() {
        try {
            pollAndPublish();
        } catch (Exception e) {
            log.error("Error in polling cycle for vendor: {}, circle: {}, table: {}. " +
                    "Task will continue on next scheduled interval.",
                    config.getVendorName(), config.getCircleName(), config.getTableName(), e);
        }
    }

    private void pollAndPublish() {
        // Safely resolve the source table name
        String safeTableName = SqlIdentifier.tableName(config.getTableName());

        // Build the SELECT query to fetch all rows with limit
        String selectQuery = String.format(
                "SELECT * FROM %s LIMIT %d",
                safeTableName,
                config.getFetchSize()
        );

        log.debug("Executing query for vendor {}, circle {}: {}",
                config.getVendorName(), config.getCircleName(), selectQuery);

        // Execute the dynamic SQL SELECT query
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(selectQuery);

        if (rows.isEmpty()) {
            log.debug("No records found for vendor: {}, table: {}", config.getVendorName(), config.getTableName());
            return;
        }

        log.info("Found {} records for vendor: {}, circle: {}, table: {}",
                rows.size(), config.getVendorName(), config.getCircleName(), config.getTableName());

        // Build Kafka topic name based on queue_name
        String kafkaTopic = config.getQueueName();

        // Send each row to Kafka
        int successCount = 0;
        for (Map<String, Object> row : rows) {
            try {
                // Send the entire row as a JSON map to Kafka
                // The message key is the queue_id for partitioning
                String messageKey = String.valueOf(config.getQueueId());
                kafkaTemplate.send(kafkaTopic, messageKey, row);
                successCount++;
                log.debug("Published row to Kafka topic: {} for vendor: {}", kafkaTopic, config.getVendorName());
            } catch (Exception e) {
                log.error("Failed to publish row to Kafka for vendor: {}", config.getVendorName(), e);
                // Continue publishing other rows
            }
        }

        log.info("Successfully published {} of {} rows to Kafka topic: {} for vendor: {}, table: {}",
                successCount, rows.size(), kafkaTopic, config.getVendorName(), config.getTableName());
    }
}

