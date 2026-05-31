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
 * when they do not yet exist. Tables include standard processing columns plus payload
 * columns derived from {@code sm_vendor_param_configuration}.
 */
@Repository
public class VendorCallbackSourceTableProvisioner {

    private static final Logger log = LoggerFactory.getLogger(VendorCallbackSourceTableProvisioner.class);

    private static final Set<String> BASE_COLUMNS = new HashSet<String>();

    static {
        BASE_COLUMNS.add("id");
        BASE_COLUMNS.add("process_status");
        BASE_COLUMNS.add("retry_count");
        BASE_COLUMNS.add("operator_id");
        BASE_COLUMNS.add("pack_id");
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
        ddl.append(SqlIdentifier.columnName("id")).append(" BIGINT NOT NULL AUTO_INCREMENT, ");
        ddl.append(SqlIdentifier.columnName("process_status")).append(" VARCHAR(20) NOT NULL DEFAULT 'NEW', ");
        ddl.append(SqlIdentifier.columnName("retry_count")).append(" INT NOT NULL DEFAULT 0, ");
        ddl.append(SqlIdentifier.columnName("operator_id")).append(" VARCHAR(50) NOT NULL, ");
        ddl.append(SqlIdentifier.columnName("pack_id")).append(" VARCHAR(50) NOT NULL");

        Set<String> added = new HashSet<String>(BASE_COLUMNS);
        for (VendorParamDefinition param : params) {
            String column = normalizeColumnName(param.getSourceField());
            if (added.add(column)) {
                ddl.append(", ").append(SqlIdentifier.columnName(column)).append(" VARCHAR(512) NULL");
            }
        }

        ddl.append(", PRIMARY KEY (").append(SqlIdentifier.columnName("id")).append(")");
        ddl.append(", KEY idx_").append(tableName).append("_process_status (")
                .append(SqlIdentifier.columnName("process_status")).append(")");
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
