package org.example.callback.repository;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.util.StringUtils;

/**
 * Resolves legacy queue {@code operator} / {@code pack_name} strings to the
 * numeric IDs stored in {@code sm_vendor_operator_mapping} and {@code sm_vendor_pack}.
 */
@Repository
public class VendorRoutingLookupRepository {

    private static final Logger log = LoggerFactory.getLogger(VendorRoutingLookupRepository.class);

    private static final List<String> OPERATOR_TABLE_CANDIDATES = Arrays.asList(
            "sm_operator", "sm_operator_master", "operator_master");
    private static final List<String> PACK_TABLE_CANDIDATES = Arrays.asList(
            "sm_pack", "sm_pack_master", "pack_master");

    private final JdbcTemplate jdbcTemplate;

    private volatile String resolvedOperatorTable;
    private volatile String resolvedOperatorIdColumn;
    private volatile String resolvedOperatorNameColumn;
    private volatile boolean operatorLookupAttempted;

    private volatile String resolvedPackTable;
    private volatile String resolvedPackIdColumn;
    private volatile String resolvedPackNameColumn;
    private volatile boolean packLookupAttempted;

    public VendorRoutingLookupRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Returns a key that exists in {@code allowedOperatorIds}: the raw queue value when it
     * already matches, otherwise the resolved operator_id from master tables.
     */
    public Optional<String> resolveOperatorKey(String operatorFromQueue, Set<String> allowedOperatorIds) {
        if (!StringUtils.hasText(operatorFromQueue) || allowedOperatorIds == null || allowedOperatorIds.isEmpty()) {
            return Optional.empty();
        }
        String trimmed = operatorFromQueue.trim();
        if (allowedOperatorIds.contains(trimmed)) {
            return Optional.of(trimmed);
        }
        Optional<String> resolved = lookupOperatorId(trimmed);
        if (resolved.isPresent() && allowedOperatorIds.contains(resolved.get())) {
            return resolved;
        }
        return Optional.empty();
    }

    /**
     * Returns a key that exists in {@code allowedPackIds}: the raw queue value when it
     * already matches, otherwise the resolved pack_id from master tables.
     */
    public Optional<String> resolvePackKey(String packFromQueue, Set<String> allowedPackIds) {
        if (!StringUtils.hasText(packFromQueue) || allowedPackIds == null || allowedPackIds.isEmpty()) {
            return Optional.empty();
        }
        String trimmed = packFromQueue.trim();
        if (allowedPackIds.contains(trimmed)) {
            return Optional.of(trimmed);
        }
        Optional<String> resolved = lookupPackId(trimmed);
        if (resolved.isPresent() && allowedPackIds.contains(resolved.get())) {
            return resolved;
        }
        return Optional.empty();
    }

    private Optional<String> lookupOperatorId(String operatorName) {
        if (!ensureOperatorLookup()) {
            return Optional.empty();
        }
        String sql = "SELECT " + resolvedOperatorIdColumn
                + " FROM " + resolvedOperatorTable
                + " WHERE LOWER(" + resolvedOperatorNameColumn + ") = LOWER(?) LIMIT 1";
        try {
            List<String> ids = jdbcTemplate.queryForList(sql, String.class, operatorName);
            if (ids.isEmpty() || !StringUtils.hasText(ids.get(0))) {
                return Optional.empty();
            }
            return Optional.of(ids.get(0).trim());
        } catch (DataAccessException ex) {
            log.debug("Operator lookup failed for {}: {}", operatorName, ex.getMessage());
            return Optional.empty();
        }
    }

    private Optional<String> lookupPackId(String packName) {
        if (!ensurePackLookup()) {
            return Optional.empty();
        }
        String sql = "SELECT " + resolvedPackIdColumn
                + " FROM " + resolvedPackTable
                + " WHERE LOWER(" + resolvedPackNameColumn + ") = LOWER(?) LIMIT 1";
        try {
            List<String> ids = jdbcTemplate.queryForList(sql, String.class, packName);
            if (ids.isEmpty() || !StringUtils.hasText(ids.get(0))) {
                return Optional.empty();
            }
            return Optional.of(ids.get(0).trim());
        } catch (DataAccessException ex) {
            log.debug("Pack lookup failed for {}: {}", packName, ex.getMessage());
            return Optional.empty();
        }
    }

    private boolean ensureOperatorLookup() {
        if (operatorLookupAttempted) {
            return resolvedOperatorTable != null;
        }
        synchronized (this) {
            if (operatorLookupAttempted) {
                return resolvedOperatorTable != null;
            }
            operatorLookupAttempted = true;
            for (String table : OPERATOR_TABLE_CANDIDATES) {
                if (tryBindOperatorTable(table)) {
                    log.info("Operator name lookup uses {}.{} -> {}.{}",
                            resolvedOperatorTable,
                            resolvedOperatorNameColumn,
                            resolvedOperatorTable,
                            resolvedOperatorIdColumn);
                    return true;
                }
            }
            log.warn("No operator master table found; operator validation uses queue values only");
            return false;
        }
    }

    private boolean ensurePackLookup() {
        if (packLookupAttempted) {
            return resolvedPackTable != null;
        }
        synchronized (this) {
            if (packLookupAttempted) {
                return resolvedPackTable != null;
            }
            packLookupAttempted = true;
            for (String table : PACK_TABLE_CANDIDATES) {
                if (tryBindPackTable(table)) {
                    log.info("Pack name lookup uses {}.{} -> {}.{}",
                            resolvedPackTable,
                            resolvedPackNameColumn,
                            resolvedPackTable,
                            resolvedPackIdColumn);
                    return true;
                }
            }
            log.warn("No pack master table found; pack validation uses queue values only");
            return false;
        }
    }

    private boolean tryBindOperatorTable(String tableName) {
        Set<String> columns = tableColumns(tableName);
        if (columns.isEmpty()) {
            return false;
        }
        String idColumn = firstPresent(columns, "operator_id", "id");
        String nameColumn = firstPresent(columns, "operator_name", "operator", "name");
        if (idColumn == null || nameColumn == null) {
            return false;
        }
        resolvedOperatorTable = tableName;
        resolvedOperatorIdColumn = idColumn;
        resolvedOperatorNameColumn = nameColumn;
        return true;
    }

    private boolean tryBindPackTable(String tableName) {
        Set<String> columns = tableColumns(tableName);
        if (columns.isEmpty()) {
            return false;
        }
        String idColumn = firstPresent(columns, "pack_id", "id");
        String nameColumn = firstPresent(columns, "pack_name", "pack", "name");
        if (idColumn == null || nameColumn == null) {
            return false;
        }
        resolvedPackTable = tableName;
        resolvedPackIdColumn = idColumn;
        resolvedPackNameColumn = nameColumn;
        return true;
    }

    private Set<String> tableColumns(String tableName) {
        try {
            List<String> columns = jdbcTemplate.queryForList(
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                            + "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
                    String.class,
                    tableName);
            if (columns.isEmpty()) {
                return Collections.emptySet();
            }
            Set<String> normalized = new LinkedHashSet<String>();
            for (String column : columns) {
                normalized.add(column.toLowerCase(Locale.ROOT));
            }
            return normalized;
        } catch (DataAccessException ex) {
            return Collections.emptySet();
        }
    }

    private static String firstPresent(Set<String> columns, String... candidates) {
        for (String candidate : candidates) {
            if (columns.contains(candidate)) {
                return candidate;
            }
        }
        return null;
    }
}
