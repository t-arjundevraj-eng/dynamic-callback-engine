package org.example.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import org.example.messaging.DeadLetterEvent;
import org.example.messaging.VendorEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class DeadLetterJdbcRepository {

    private static final String INSERT_SQL =
            "INSERT INTO vendor_dead_letters " +
                    "(event_id, vendor_name, schema_version, payload_json, source_topic, source_partition, " +
                    "source_offset, error_type, error_message, retry_count, failed_at) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public DeadLetterJdbcRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    public void save(DeadLetterEvent deadLetterEvent) {
        try {
            VendorEvent event = deadLetterEvent.getOriginalEvent();
            jdbcTemplate.update(
                    INSERT_SQL,
                    event == null ? null : event.getEventId(),
                    event == null ? null : event.getVendor(),
                    event == null ? null : event.getSchemaVersion(),
                    objectMapper.writeValueAsString(event),
                    deadLetterEvent.getSourceTopic(),
                    deadLetterEvent.getSourcePartition(),
                    deadLetterEvent.getSourceOffset(),
                    deadLetterEvent.getErrorType(),
                    deadLetterEvent.getErrorMessage(),
                    deadLetterEvent.getRetryCount(),
                    Timestamp.from(deadLetterEvent.getFailedAt())
            );
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Unable to serialize dead-letter payload", ex);
        }
    }
}
