package org.example.callback.repository;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.example.callback.dto.ProcessStatus;
import org.example.callback.dto.QueueRowStateUpdate;
import org.example.persistence.SqlIdentifier;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class VendorSourceQueueJdbcRepository {

    private static final String STATUS_COLUMN = "process_status";
    private static final String RETRY_COUNT_COLUMN = "retry_count";

    private final JdbcTemplate jdbcTemplate;

    public VendorSourceQueueJdbcRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Map<String, Object>> pollUnprocessedRows(String tableName, int fetchSize) {
        String safeTable = SqlIdentifier.tableName(tableName);
        SqlIdentifier.columnName(STATUS_COLUMN);
        int limit = Math.max(1, fetchSize);
        String sql = "SELECT * FROM " + safeTable
                + " WHERE " + SqlIdentifier.columnName(STATUS_COLUMN) + " IN (?, ?)"
                + " ORDER BY " + SqlIdentifier.columnName("id")
                + " LIMIT " + limit;
        return jdbcTemplate.queryForList(sql, ProcessStatus.NEW, ProcessStatus.RETRY);
    }

    @Transactional
    public int[] bulkUpdateRowStates(String tableName, List<QueueRowStateUpdate> updates) {
        if (updates == null || updates.isEmpty()) {
            return new int[0];
        }
        String safeTable = SqlIdentifier.tableName(tableName);
        String sql = "UPDATE " + safeTable
                + " SET " + SqlIdentifier.columnName(STATUS_COLUMN) + " = ?, "
                + SqlIdentifier.columnName(RETRY_COUNT_COLUMN) + " = ?"
                + " WHERE " + SqlIdentifier.columnName("id") + " = ?";

        return jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                QueueRowStateUpdate update = updates.get(i);
                ps.setString(1, update.getProcessStatus());
                ps.setInt(2, update.getRetryCount());
                ps.setLong(3, update.getRowId());
            }

            @Override
            public int getBatchSize() {
                return updates.size();
            }
        });
    }
}
