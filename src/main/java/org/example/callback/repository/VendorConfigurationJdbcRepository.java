package org.example.callback.repository;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.example.callback.dto.VendorConfigurationRow;
import org.example.callback.dto.VendorParamDefinition;
import org.example.callback.util.LegacyCallbackParamMapper;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.util.StringUtils;

@Repository
public class VendorConfigurationJdbcRepository {

    private static final String CALLBACK_MODULE = "CallbackManagement";

    private static final String RESOLVED_VENDOR_JOIN =
            "SELECT vm.vendor_id AS vendorId, "
                    + "vm.vendor_name AS vendorName, "
                    + "vm.username AS username, "
                    + "vm.password AS password, "
                    + "vcc.circle AS circle, "
                    + "vcc.callback_url AS callbackUrl, "
                    + "vcc.channel_url AS channelUrl, "
                    + "COALESCE(vcc.http_method, 'GET') AS httpMethod, "
                    + "vqc.queue_id AS queueId, "
                    + "vqc.queue_name AS queueName, "
                    + "vqc.table_name AS sourceTableName, "
                    + "vqc.fetch_size AS fetchSize, "
                    + "vqc.producer_sleep_time AS producerSleepTimeMs, "
                    + "vqc.max_retry_count AS maxRetryCount "
                    + "FROM sm_vendor_master vm "
                    + "INNER JOIN sm_vendor_callback_config vcc ON vcc.vendor_id = vm.vendor_id "
                    + "INNER JOIN vendor_callback_queue_config vqc ON vqc.vendor_name = vm.vendor_name "
                    + "AND vqc.status = 1 "
                    + "AND (COALESCE(vqc.vendor_circle_flag, 0) = 0 "
                    + "     OR (vqc.circle_name IS NOT NULL AND vqc.circle_name = vcc.circle)) "
                    + "WHERE vm.isCallback_active = 1 "
                    + "AND vcc.callback_url IS NOT NULL "
                    + "AND TRIM(vcc.callback_url) <> '' "
                    + "AND EXISTS ( "
                    + "    SELECT 1 FROM sm_vendor_pack vp "
                    + "    WHERE vp.vendor_id = vm.vendor_id AND vp.is_active = 1 "
                    + ")";

    private static final String OPERATORS_BY_VENDOR =
            "SELECT operator_id FROM sm_vendor_operator_mapping WHERE vendor_id = ?";

    private static final String PACKS_BY_VENDOR =
            "SELECT pack_id FROM sm_vendor_pack WHERE vendor_id = ? AND is_active = 1";

    private static final String PARAMS_BY_VENDOR_CIRCLE =
            "SELECT param FROM sm_vendor_param_configuration "
                    + "WHERE vendor_name = (SELECT vendor_name FROM sm_vendor_master WHERE vendor_id = ?) "
                    + "AND (circle_name IS NULL OR circle_name = ?) "
                    + "ORDER BY id";

    private static final String IPS_BY_VENDOR =
            "SELECT ip_address FROM sm_vendor_ip_mapping WHERE vendor_id = ?";

    private static final String NOTIFICATION_STATUSES_BY_VENDOR =
            "SELECT GROUP_CONCAT(DISTINCT status) AS statusList "
                    + "FROM sm_vendor_notification_config "
                    + "WHERE module = ? AND vendor = ?";

    private final JdbcTemplate jdbcTemplate;
    private final BeanPropertyRowMapper<VendorConfigurationRow> configurationRowMapper;

    public VendorConfigurationJdbcRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.configurationRowMapper = new BeanPropertyRowMapper<>(VendorConfigurationRow.class);
    }

    public List<VendorConfigurationRow> findResolvedVendorRows() {
        return jdbcTemplate.query(RESOLVED_VENDOR_JOIN, configurationRowMapper);
    }

    public List<String> findOperatorIds(int vendorId) {
        return jdbcTemplate.queryForList(OPERATORS_BY_VENDOR, String.class, vendorId);
    }

    public List<String> findActivePackIds(int vendorId) {
        return jdbcTemplate.queryForList(PACKS_BY_VENDOR, String.class, vendorId);
    }

    public List<VendorParamDefinition> findParamDefinitions(int vendorId, String circle) {
        List<String> rows = jdbcTemplate.queryForList(
                PARAMS_BY_VENDOR_CIRCLE, String.class, vendorId, circle);
        if (rows.isEmpty()) {
            return Collections.emptyList();
        }

        List<VendorParamDefinition> definitions = new ArrayList<VendorParamDefinition>();
        Set<String> seen = new LinkedHashSet<String>();
        for (String row : rows) {
            if (!StringUtils.hasText(row)) {
                continue;
            }
            for (String token : row.split(",")) {
                String sourceField = token.trim();
                if (sourceField.isEmpty() || !seen.add(sourceField)) {
                    continue;
                }
                definitions.add(new VendorParamDefinition(
                        LegacyCallbackParamMapper.toUrlParamKey(sourceField),
                        sourceField,
                        false));
            }
        }
        return definitions;
    }

    public List<String> findAllowedIpAddresses(int vendorId) {
        return jdbcTemplate.queryForList(IPS_BY_VENDOR, String.class, vendorId);
    }

    public Set<String> findAllowedNotificationStatuses(int vendorId) {
        String statusList = jdbcTemplate.query(
                NOTIFICATION_STATUSES_BY_VENDOR,
                rs -> rs.next() ? rs.getString("statusList") : null,
                CALLBACK_MODULE,
                vendorId);
        if (!StringUtils.hasText(statusList)) {
            return Collections.emptySet();
        }
        Set<String> statuses = new LinkedHashSet<String>();
        statuses.addAll(Arrays.asList(statusList.split(",")));
        return statuses;
    }
}
