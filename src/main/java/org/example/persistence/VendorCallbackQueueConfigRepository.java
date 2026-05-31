package org.example.persistence;

import java.util.List;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class VendorCallbackQueueConfigRepository {

    private static final String SELECT_ACTIVE =
            "SELECT queue_id AS queueId, queue_name AS queueName, cons_pool_size AS consPoolSize, " +
                    "prod_block_queue_size AS prodBlockQueueSize, cons_block_queue_size AS consBlockQueueSize, " +
                    "fetch_size AS fetchSize, producer_sleep_time AS producerSleepTime, " +
                    "consumer_sleep_time AS consumerSleepTime, status AS active, refetch_interval AS refetchInterval, " +
                    "vendor_circle_flag AS vendorCircleFlag, vendor_name AS vendorName, circle_name AS circleName, " +
                    "max_retry_count AS maxRetryCount, table_name AS tableName " +
                    "FROM vendor_callback_queue_config WHERE status = 1";

    private static final String SELECT_BY_VENDOR =
            SELECT_ACTIVE + " AND vendor_name = ? LIMIT 1";

    private static final String SELECT_BY_QUEUE_NAME =
            SELECT_ACTIVE + " AND queue_name = ? LIMIT 1";

    private final JdbcTemplate jdbcTemplate;

    public VendorCallbackQueueConfigRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<VendorCallbackQueueConfig> findActive() {
        return jdbcTemplate.query(SELECT_ACTIVE, new BeanPropertyRowMapper<>(VendorCallbackQueueConfig.class));
    }

    public VendorCallbackQueueConfig findActiveByVendorName(String vendorName) {
        List<VendorCallbackQueueConfig> configs = jdbcTemplate.query(
                SELECT_BY_VENDOR,
                new BeanPropertyRowMapper<>(VendorCallbackQueueConfig.class),
                vendorName
        );
        if (configs.isEmpty()) {
            return null;
        }
        return configs.get(0);
    }

    public VendorCallbackQueueConfig findActiveByQueueName(String queueName) {
        List<VendorCallbackQueueConfig> configs = jdbcTemplate.query(
                SELECT_BY_QUEUE_NAME,
                new BeanPropertyRowMapper<>(VendorCallbackQueueConfig.class),
                queueName
        );
        if (configs.isEmpty()) {
            return null;
        }
        return configs.get(0);
    }
}
