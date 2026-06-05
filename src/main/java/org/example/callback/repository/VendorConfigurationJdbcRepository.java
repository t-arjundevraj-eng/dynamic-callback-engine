package org.example.callback.repository;

import java.util.List;
import org.example.callback.dto.VendorConfigurationRow;
import org.example.callback.dto.VendorParamDefinition;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class VendorConfigurationJdbcRepository {

    private static final String RESOLVED_VENDOR_JOIN =
            "SELECT vm.vendor_id AS vendorId, "
                    + "vm.vendor_name AS vendorName, "
                    + "vcc.circle AS circle, "
                    + "vcc.callback_url AS callbackUrl, "
                    + "vcc.channel_url AS channelUrl, "
                    + "'POST' AS httpMethod, " // Safe hardcoded string literal fallback
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
                    + "WHERE vm.isCallback_active = 1 " // Matches remote DB underscore spelling
                    + "AND vcc.callback_url IS NOT NULL "
                    + "AND TRIM(vcc.callback_url) <> '' "
                    + "AND EXISTS ( "
                    + "    SELECT 1 FROM sm_vendor_pack vp "
                    + "    WHERE vp.vendor_id = vm.vendor_id AND vp.is_active = 1 " // Matches remote DB underscore spelling
                    + ")";

    private static final String OPERATORS_BY_VENDOR =
            "SELECT operator_id FROM sm_vendor_operator_mapping WHERE vendor_id = ?";

    private static final String PACKS_BY_VENDOR =
            "SELECT pack_id FROM sm_vendor_pack WHERE vendor_id = ? AND is_active = 1"; 

    private static final String PARAMS_BY_VENDOR_CIRCLE =
            "SELECT param AS paramKey, param AS sourceField, " // Maps 'param' column to both fields to prevent RowMapper crashes
                    + "1 AS required "
                    + "FROM sm_vendor_param_configuration "
                    + "WHERE vendor_name = (SELECT vendor_name FROM sm_vendor_master WHERE vendor_id = ?) " 
                    + "AND (circle_name IS NULL OR circle_name = ?) " // Fixed to use circle_name
                    + "ORDER BY id";

    private static final String IPS_BY_VENDOR =
            "SELECT ip FROM sm_vendor_ip_mapping WHERE vendor_id = ?"; // Fixed to use ip column

    private final JdbcTemplate jdbcTemplate;
    private final BeanPropertyRowMapper<VendorConfigurationRow> configurationRowMapper;
    private final BeanPropertyRowMapper<VendorParamDefinition> paramRowMapper;

    public VendorConfigurationJdbcRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.configurationRowMapper = new BeanPropertyRowMapper<>(VendorConfigurationRow.class);
        this.paramRowMapper = BeanPropertyRowMapper.newInstance(VendorParamDefinition.class);
        this.paramRowMapper.setPrimitivesDefaultedForNullValue(true);
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
        return jdbcTemplate.query(PARAMS_BY_VENDOR_CIRCLE, paramRowMapper, vendorId, circle);
    }

    public List<String> findAllowedIpAddresses(int vendorId) {
        return jdbcTemplate.queryForList(IPS_BY_VENDOR, String.class, vendorId);
    }
}