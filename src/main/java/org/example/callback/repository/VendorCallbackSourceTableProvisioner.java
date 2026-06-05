package org.example.callback.repository;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.example.callback.dto.VendorParamDefinition;
import org.example.persistence.SqlIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Creates source queue tables named in {@code vendor_callback_queue_config.table_name}
 * when they do not yet exist, using the legacy PRBT queue column layout.
 */
@Repository
public class VendorCallbackSourceTableProvisioner {

    private static final Logger log = LoggerFactory.getLogger(VendorCallbackSourceTableProvisioner.class);

    private static final Set<String> BASE_COLUMNS = new HashSet<String>();

    static {
        BASE_COLUMNS.add("request_id");
        BASE_COLUMNS.add("callback_status");
        BASE_COLUMNS.add("retry_count");
        BASE_COLUMNS.add("operator");
        BASE_COLUMNS.add("pack_name");
        BASE_COLUMNS.add("msisdn");
        BASE_COLUMNS.add("next_retry_time");
    }

    private final JdbcTemplate jdbcTemplate;
    private final VendorConfigurationJdbcRepository configurationRepository;

    public VendorCallbackSourceTableProvisioner(
            JdbcTemplate jdbcTemplate,
            VendorConfigurationJdbcRepository configurationRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.configurationRepository = configurationRepository;
    }

    public void ensureSourceTable(int vendorId, String tableName, String circle) {
        SqlIdentifier.tableName(tableName);
        if (tableExists(tableName)) {
            log.debug("Source queue table already exists: {}", tableName);
            return;
        }

        List<VendorParamDefinition> params = configurationRepository.findParamDefinitions(vendorId, circle);
        String ddl = buildCreateTableDdl(tableName, params);
        jdbcTemplate.execute(ddl);
        log.info("Created source queue table {} for vendorId={} circle={}", tableName, vendorId, circle);
    }

    private String buildCreateTableDdl(String tableName, List<VendorParamDefinition> params) {
        String safeTable = SqlIdentifier.tableName(tableName);
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(safeTable).append(" (");
        ddl.append(SqlIdentifier.columnName("request_id")).append(" VARCHAR(120) NOT NULL, ");
        ddl.append(SqlIdentifier.columnName("callback_status")).append(" VARCHAR(20) NOT NULL DEFAULT '0', ");
        ddl.append(SqlIdentifier.columnName("retry_count")).append(" INT NOT NULL DEFAULT 0, ");
        ddl.append(SqlIdentifier.columnName("next_retry_time")).append(" DATETIME DEFAULT NULL, ");
        ddl.append(SqlIdentifier.columnName("operator")).append(" VARCHAR(50) DEFAULT NULL, ");
        ddl.append(SqlIdentifier.columnName("pack_name")).append(" VARCHAR(50) DEFAULT NULL, ");
        ddl.append(SqlIdentifier.columnName("msisdn")).append(" VARCHAR(50) DEFAULT NULL, ");
        ddl.append(SqlIdentifier.columnName("action")).append(" VARCHAR(50) DEFAULT NULL, ");
        ddl.append(SqlIdentifier.columnName("channel")).append(" VARCHAR(50) DEFAULT NULL, ");
        ddl.append(SqlIdentifier.columnName("info")).append(" VARCHAR(255) DEFAULT NULL, ");
        ddl.append(SqlIdentifier.columnName("circle")).append(" VARCHAR(50) DEFAULT NULL, ");
        ddl.append(SqlIdentifier.columnName("start_date")).append(" DATETIME DEFAULT NULL, ");
        ddl.append(SqlIdentifier.columnName("end_date")).append(" DATETIME DEFAULT NULL, ");
        ddl.append(SqlIdentifier.columnName("transaction_id")).append(" VARCHAR(120) DEFAULT NULL, ");
        ddl.append(SqlIdentifier.columnName("price_point_charged")).append(" VARCHAR(64) DEFAULT NULL, ");
        ddl.append(SqlIdentifier.columnName("status")).append(" VARCHAR(20) DEFAULT NULL, ");
        ddl.append(SqlIdentifier.columnName("vendor_name")).append(" VARCHAR(120) DEFAULT NULL, ");
        ddl.append(SqlIdentifier.columnName("request_time")).append(" DATETIME DEFAULT NULL");

        Set<String> added = new HashSet<String>(BASE_COLUMNS);
        added.add("action");
        added.add("channel");
        added.add("info");
        added.add("circle");
        added.add("start_date");
        added.add("end_date");
        added.add("transaction_id");
        added.add("price_point_charged");
        added.add("status");
        added.add("vendor_name");
        added.add("request_time");

        for (VendorParamDefinition param : params) {
            String column = normalizeColumnName(param.getSourceField());
            if (added.add(column)) {
                ddl.append(", ").append(SqlIdentifier.columnName(column)).append(" VARCHAR(512) NULL");
            }
        }

        ddl.append(", PRIMARY KEY (").append(SqlIdentifier.columnName("request_id")).append(")");
        ddl.append(", KEY idx_").append(tableName).append("_callback_status (")
                .append(SqlIdentifier.columnName("callback_status")).append(")");
        ddl.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8");
        return ddl.toString();
    }

    private static String normalizeColumnName(String sourceField) {
        SqlIdentifier.columnName(sourceField);
        return sourceField;
    }

    private boolean tableExists(String tableName) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM information_schema.tables "
                        + "WHERE table_schema = DATABASE() AND table_name = ?",
                Integer.class,
                tableName);
        return count != null && count > 0;
    }
}
