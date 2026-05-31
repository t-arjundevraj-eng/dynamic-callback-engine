package org.example.persistence;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.example.messaging.VendorEvent;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class VendorEventJdbcRepository {

    private final JdbcTemplate jdbcTemplate;
    private final VendorTableMetadataRepository tableMetadataRepository;

    public VendorEventJdbcRepository(
            JdbcTemplate jdbcTemplate,
            VendorTableMetadataRepository tableMetadataRepository
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.tableMetadataRepository = tableMetadataRepository;
    }

    public void save(VendorCallbackQueueConfig config, VendorEvent event) {
        try {
            Set<String> tableColumns = tableMetadataRepository.columns(config.getTableName());
            Map<String, Object> fields = event.getFields();
            List<String> insertColumns = fields.keySet().stream()
                    .filter(tableColumns::contains)
                    .collect(Collectors.toList());
            if (insertColumns.isEmpty()) {
                throw new IllegalArgumentException("No payload fields match target table " + config.getTableName());
            }

            String columns = insertColumns.stream()
                    .map(SqlIdentifier::columnName)
                    .collect(Collectors.joining(", "));
            String placeholders = insertColumns.stream()
                    .map(column -> "?")
                    .collect(Collectors.joining(", "));
            String sql = "INSERT IGNORE INTO " + SqlIdentifier.tableName(config.getTableName())
                    + " (" + columns + ") VALUES (" + placeholders + ")";

            List<Object> values = new ArrayList<>();
            for (String column : insertColumns) {
                values.add(fields.get(column));
            }
            jdbcTemplate.update(sql, values.toArray());
        } catch (DataAccessException ex) {
            throw ex;
        }
    }
}
