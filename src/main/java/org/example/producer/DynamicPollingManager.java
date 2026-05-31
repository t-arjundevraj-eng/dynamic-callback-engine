package org.example.producer;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.example.persistence.VendorCallbackQueueConfig;
import org.example.persistence.VendorCallbackQueueConfigRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * Dynamically manages polling tasks for vendor-specific database tables.
 * Fetches active vendor configurations from vendor_callback_queue_config table
 * and schedules polling tasks to read from source tables and publish to Kafka.
 */
@Service
@Slf4j
public class DynamicPollingManager {

    private final VendorCallbackQueueConfigRepository configRepository;
    private final JdbcTemplate jdbcTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private ThreadPoolTaskScheduler taskScheduler;

    public DynamicPollingManager(
            VendorCallbackQueueConfigRepository configRepository,
            JdbcTemplate jdbcTemplate,
            KafkaTemplate<String, Object> kafkaTemplate) {
        this.configRepository = configRepository;
        this.jdbcTemplate = jdbcTemplate;
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostConstruct
    public void initializePolling() {
        log.info("Initializing DynamicPollingManager");

        // Initialize the thread pool scheduler
        taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setPoolSize(20);
        taskScheduler.setThreadNamePrefix("vendor-poller-");
        taskScheduler.setWaitForTasksToCompleteOnShutdown(true);
        taskScheduler.setAwaitTerminationSeconds(60);
        taskScheduler.initialize();

        // Fetch all active configurations from vendor_callback_queue_config
        List<VendorCallbackQueueConfig> activeConfigs = configRepository.findActive();
        log.info("Found {} active vendor queue configurations", activeConfigs.size());

        // Schedule a polling task for each active configuration
        for (VendorCallbackQueueConfig config : activeConfigs) {
            VendorQueuePollerTask pollerTask = new VendorQueuePollerTask(
                    config,
                    jdbcTemplate,
                    kafkaTemplate
            );

            Long sleepTime = config.getProducerSleepTime();
            log.info("Scheduling poller for vendor: {}, circle: {}, source table: {}, sleep interval: {} ms",
                    config.getVendorName(), config.getCircleName(), config.getTableName(), sleepTime);

            taskScheduler.scheduleWithFixedDelay(pollerTask, sleepTime);
        }

        log.info("DynamicPollingManager initialization complete with {} polling tasks", activeConfigs.size());
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down DynamicPollingManager");
        if (taskScheduler != null) {
            taskScheduler.shutdown();
            log.info("Task scheduler shutdown complete");
        }
    }
}

