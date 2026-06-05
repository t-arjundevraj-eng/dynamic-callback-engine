package org.example.callback.repository;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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
 * Resolves legacy queue {@code operator} / {@code pack_name} strings and expands vendor
 * allow-lists with master-table names (production uses numeric IDs in mapping tables).
 */
@Repository
public class VendorRoutingLookupRepository {

    private static final Logger log = LoggerFactory.getLogger(VendorRoutingLookupRepository.class);

    private static final List<String> OPERATOR_TABLE_CANDIDATES = Arrays.asList(
            "sm_operator", "sm_operator_master", "operator_master", "operator_details",
            "sm_operator_details", "tbl_operator", "operator");
    private static final List<String> PACK_TABLE_CANDIDATES = Arrays.asList(
            "sm_pack", "sm_pack_master", "pack_master", "pack_details", "sm_pack_details",
            "tbl_pack", "pack");

    private final JdbcTemplate jdbcTemplate;

    private volatile MasterTableBinding operatorBinding;
    private volatile boolean operatorBindingAttempted;

    private volatile MasterTableBinding packBinding;
    private volatile boolean packBindingAttempted;

    public VendorRoutingLookupRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Adds operator names/codes from master tables for IDs already mapped to the vendor.
     */
    public Set<String> expandAllowedOperatorKeys(int vendorId, Set<String> operatorIds) {
        Set<String> keys = new LinkedHashSet<String>();
        if (operatorIds != null) {
            keys.addAll(operatorIds);
        }
        MasterTableBinding binding = operatorBinding();
        if (binding == null || operatorIds == null || operatorIds.isEmpty()) {
            return keys;
        }
        String sql = "SELECT DISTINCT o." + binding.nameColumn
                + " FROM sm_vendor_operator_mapping m"
                + " INNER JOIN " + binding.tableName + " o"
                + " ON CAST(m.operator_id AS CHAR) = CAST(o." + binding.idColumn + " AS CHAR)"
                + " WHERE m.vendor_id = ?";
        try {
            keys.addAll(jdbcTemplate.queryForList(sql, String.class, vendorId));
            log.info("Expanded operator allow-list for vendorId={} using {} ({} keys)",
                    vendorId, binding.tableName, keys.size());
        } catch (DataAccessException ex) {
            log.warn("Could not expand operator allow-list for vendorId={} via {}: {}",
                    vendorId, binding.tableName, ex.getMessage());
        }
        return keys;
    }

    /**
     * Adds pack names from master tables (and {@code sm_vendor_pack.pack_name} when present)
     * for pack IDs already mapped to the vendor.
     */
    public Set<String> expandAllowedPackKeys(int vendorId, Set<String> packIds) {
        Set<String> keys = new LinkedHashSet<String>();
        if (packIds != null) {
            keys.addAll(packIds);
        }
        keys.addAll(vendorPackNames(vendorId));

        MasterTableBinding binding = packBinding();
        if (binding == null || packIds == null || packIds.isEmpty()) {
            return keys;
        }
        String sql = "SELECT DISTINCT p." + binding.nameColumn
                + " FROM sm_vendor_pack vp"
                + " INNER JOIN " + binding.tableName + " p"
                + " ON CAST(vp.pack_id AS CHAR) = CAST(p." + binding.idColumn + " AS CHAR)"
                + " WHERE vp.vendor_id = ? AND vp.is_active = 1";
        try {
            keys.addAll(jdbcTemplate.queryForList(sql, String.class, vendorId));
            log.info("Expanded pack allow-list for vendorId={} using {} ({} keys)",
                    vendorId, binding.tableName, keys.size());
        } catch (DataAccessException ex) {
            log.warn("Could not expand pack allow-list for vendorId={} via {}: {}",
                    vendorId, binding.tableName, ex.getMessage());
        }
        return keys;
    }

    public Optional<String> resolveOperatorKey(String operatorFromQueue, Set<String> allowedOperatorKeys) {
        if (!StringUtils.hasText(operatorFromQueue) || allowedOperatorKeys == null || allowedOperatorKeys.isEmpty()) {
            return Optional.empty();
        }
        String trimmed = operatorFromQueue.trim();
        if (containsIgnoreCase(allowedOperatorKeys, trimmed)) {
            return Optional.of(trimmed);
        }
        Optional<String> resolved = lookupOperatorId(trimmed);
        if (resolved.isPresent() && containsIgnoreCase(allowedOperatorKeys, resolved.get())) {
            return resolved;
        }
        return Optional.empty();
    }

    public Optional<String> resolvePackKey(String packFromQueue, Set<String> allowedPackKeys) {
        if (!StringUtils.hasText(packFromQueue) || allowedPackKeys == null || allowedPackKeys.isEmpty()) {
            return Optional.empty();
        }
        String trimmed = packFromQueue.trim();
        if (containsIgnoreCase(allowedPackKeys, trimmed)) {
            return Optional.of(trimmed);
        }
        Optional<String> resolved = lookupPackId(trimmed);
        if (resolved.isPresent() && containsIgnoreCase(allowedPackKeys, resolved.get())) {
            return resolved;
        }
        return Optional.empty();
    }

    private Optional<String> lookupOperatorId(String operatorName) {
        MasterTableBinding binding = operatorBinding();
        if (binding == null) {
            return Optional.empty();
        }
        return lookupId(binding, operatorName);
    }

    private Optional<String> lookupPackId(String packName) {
        MasterTableBinding binding = packBinding();
        if (binding == null) {
            return Optional.empty();
        }
        return lookupId(binding, packName);
    }

    private Optional<String> lookupId(MasterTableBinding binding, String nameValue) {
        String sql = "SELECT " + binding.idColumn
                + " FROM " + binding.tableName
                + " WHERE LOWER(" + binding.nameColumn + ") = LOWER(?) LIMIT 1";
        try {
            List<String> ids = jdbcTemplate.queryForList(sql, String.class, nameValue);
            if (ids.isEmpty() || !StringUtils.hasText(ids.get(0))) {
                return Optional.empty();
            }
            return Optional.of(ids.get(0).trim());
        } catch (DataAccessException ex) {
            log.debug("Lookup failed in {} for {}: {}", binding.tableName, nameValue, ex.getMessage());
            return Optional.empty();
        }
    }

    private Set<String> vendorPackNames(int vendorId) {
        Set<String> columns = tableColumns("sm_vendor_pack");
        if (!columns.contains("pack_name")) {
            return Collections.emptySet();
        }
        try {
            return new LinkedHashSet<String>(jdbcTemplate.queryForList(
                    "SELECT DISTINCT pack_name FROM sm_vendor_pack"
                            + " WHERE vendor_id = ? AND is_active = 1 AND pack_name IS NOT NULL"
                            + " AND TRIM(pack_name) <> ''",
                    String.class,
                    vendorId));
        } catch (DataAccessException ex) {
            log.debug("sm_vendor_pack.pack_name lookup skipped for vendorId={}: {}", vendorId, ex.getMessage());
            return Collections.emptySet();
        }
    }

    private MasterTableBinding operatorBinding() {
        if (operatorBindingAttempted) {
            return operatorBinding;
        }
        synchronized (this) {
            if (operatorBindingAttempted) {
                return operatorBinding;
            }
            operatorBindingAttempted = true;
            operatorBinding = discoverBinding(OPERATOR_TABLE_CANDIDATES, true);
            if (operatorBinding != null) {
                log.info("Operator master table: {} ({} -> {})",
                        operatorBinding.tableName,
                        operatorBinding.nameColumn,
                        operatorBinding.idColumn);
            } else {
                log.warn("No operator master table found; operator validation uses mapped IDs and queue values only");
            }
            return operatorBinding;
        }
    }

    private MasterTableBinding packBinding() {
        if (packBindingAttempted) {
            return packBinding;
        }
        synchronized (this) {
            if (packBindingAttempted) {
                return packBinding;
            }
            packBindingAttempted = true;
            packBinding = discoverBinding(PACK_TABLE_CANDIDATES, false);
            if (packBinding != null) {
                log.info("Pack master table: {} ({} -> {})",
                        packBinding.tableName,
                        packBinding.nameColumn,
                        packBinding.idColumn);
            } else {
                log.warn("No pack master table found; pack validation uses mapped IDs and queue values only");
            }
            return packBinding;
        }
    }

    private MasterTableBinding discoverBinding(List<String> preferredTables, boolean operator) {
        for (String table : preferredTables) {
            MasterTableBinding binding = bindTable(table, operator);
            if (binding != null) {
                return binding;
            }
        }
        List<String> discovered = discoverTablesFromSchema(operator);
        for (String table : discovered) {
            MasterTableBinding binding = bindTable(table, operator);
            if (binding != null) {
                return binding;
            }
        }
        return null;
    }

    private List<String> discoverTablesFromSchema(boolean operator) {
        String idColumn = operator ? "operator_id" : "pack_id";
        String namePattern = operator ? "operator%" : "pack%";
        try {
            List<String> tables = jdbcTemplate.queryForList(
                    "SELECT DISTINCT c1.TABLE_NAME"
                            + " FROM INFORMATION_SCHEMA.COLUMNS c1"
                            + " INNER JOIN INFORMATION_SCHEMA.COLUMNS c2"
                            + " ON c1.TABLE_SCHEMA = c2.TABLE_SCHEMA AND c1.TABLE_NAME = c2.TABLE_NAME"
                            + " WHERE c1.TABLE_SCHEMA = DATABASE()"
                            + " AND c1.COLUMN_NAME = ?"
                            + " AND c2.COLUMN_NAME LIKE ?"
                            + " ORDER BY c1.TABLE_NAME",
                    String.class,
                    idColumn,
                    namePattern);
            List<String> ranked = new ArrayList<String>(tables);
            ranked.sort(bindingTableComparator());
            return ranked;
        } catch (DataAccessException ex) {
            log.debug("Schema discovery failed for {} master tables: {}",
                    operator ? "operator" : "pack", ex.getMessage());
            return Collections.emptyList();
        }
    }

    private Comparator<String> bindingTableComparator() {
        return new Comparator<String>() {
            @Override
            public int compare(String left, String right) {
                return Integer.compare(score(right), score(left));
            }

            private int score(String table) {
                String lower = table.toLowerCase(Locale.ROOT);
                int score = 0;
                if (lower.startsWith("sm_")) {
                    score += 4;
                }
                if (lower.contains("operator") || lower.contains("pack")) {
                    score += 2;
                }
                if (lower.contains("master") || lower.contains("details")) {
                    score += 1;
                }
                if (lower.contains("vendor") || lower.contains("callback") || lower.contains("queue")) {
                    score -= 3;
                }
                return score;
            }
        };
    }

    private MasterTableBinding bindTable(String tableName, boolean operator) {
        Set<String> columns = tableColumns(tableName);
        if (columns.isEmpty()) {
            return null;
        }
        String idColumn = firstPresent(columns,
                operator ? "operator_id" : "pack_id",
                "id");
        String nameColumn = firstPresent(columns,
                operator ? "operator_name" : "pack_name",
                operator ? "operator" : "pack",
                "name");
        if (idColumn == null || nameColumn == null || idColumn.equals(nameColumn)) {
            return null;
        }
        return new MasterTableBinding(tableName, idColumn, nameColumn);
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

    private static boolean containsIgnoreCase(Set<String> values, String candidate) {
        for (String value : values) {
            if (value != null && value.equalsIgnoreCase(candidate)) {
                return true;
            }
        }
        return false;
    }

    private static String firstPresent(Set<String> columns, String... candidates) {
        for (String candidate : candidates) {
            if (columns.contains(candidate)) {
                return candidate;
            }
        }
        return null;
    }

    private static final class MasterTableBinding {
        private final String tableName;
        private final String idColumn;
        private final String nameColumn;

        private MasterTableBinding(String tableName, String idColumn, String nameColumn) {
            this.tableName = tableName;
            this.idColumn = idColumn;
            this.nameColumn = nameColumn;
        }
    }
}
