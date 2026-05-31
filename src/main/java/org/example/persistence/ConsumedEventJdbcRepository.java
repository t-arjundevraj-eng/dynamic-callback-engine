package org.example.persistence;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;
import org.example.messaging.TelemetryEvent;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ConsumedEventJdbcRepository {

    private static final String INSERT_SQL =
            "INSERT IGNORE INTO consumed_events " +
                    "(event_id, producer_id, sequence_number, payload, generated_at, consumed_at) " +
                    "VALUES (?, ?, ?, ?, ?, ?)";

    private final JdbcTemplate jdbcTemplate;

    public ConsumedEventJdbcRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void saveBatch(List<TelemetryEvent> events) {
        Timestamp consumedAt = new Timestamp(System.currentTimeMillis());
        jdbcTemplate.batchUpdate(INSERT_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws java.sql.SQLException {
                TelemetryEvent event = events.get(i);
                ps.setString(1, event.getEventId());
                ps.setInt(2, event.getProducerId());
                ps.setLong(3, event.getSequenceNumber());
                ps.setString(4, event.getPayload());
                ps.setTimestamp(5, Timestamp.from(event.getGeneratedAt()));
                ps.setTimestamp(6, consumedAt);
            }

            @Override
            public int getBatchSize() {
                return events.size();
            }
        });
    }
}
