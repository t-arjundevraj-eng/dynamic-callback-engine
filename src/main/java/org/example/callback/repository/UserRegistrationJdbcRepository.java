package org.example.callback.repository;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.example.persistence.SqlIdentifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.util.StringUtils;

/**
 * Loads param1/param2/param3 from operator-specific user registration tables
 * (e.g. sm_user_registration_vodacom), matching legacy CallbackDaoImpl behaviour.
 */
@Repository
public class UserRegistrationJdbcRepository {

    private static final String[] REGISTRATION_COLUMNS = {"param1", "param2", "param3"};

    private final JdbcTemplate jdbcTemplate;

    public UserRegistrationJdbcRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Map<String, String> findRegistrationParams(
            String operator,
            String msisdn,
            String packName,
            String circle) {
        if (!StringUtils.hasText(operator) || !StringUtils.hasText(msisdn)) {
            return Collections.emptyMap();
        }
        String tableName = "sm_user_registration_" + operator.trim().toLowerCase();
        if (!tableExists(tableName)) {
            return Collections.emptyMap();
        }

        String safeTable = SqlIdentifier.tableName(tableName);
        String sql = "SELECT param1, param2, param3 FROM " + safeTable
                + " WHERE msisdn = ? AND pack_name = ? AND circle = ? LIMIT 1";

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(
                sql, msisdn, packName, circle);
        if (rows.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> params = new LinkedHashMap<String, String>();
        Map<String, Object> row = rows.get(0);
        for (String column : REGISTRATION_COLUMNS) {
            Object value = row.get(column);
            if (value != null) {
                params.put(column, String.valueOf(value));
            }
        }
        return params;
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
