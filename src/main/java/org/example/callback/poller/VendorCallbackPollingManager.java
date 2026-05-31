package org.example.callback.poller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.example.callback.config.VendorCallbackProperties;
import org.example.callback.dto.ResolvedVendorConfiguration;
import org.example.callback.repository.VendorSourceQueueJdbcRepository;
import org.example.callback.service.QueueRowStateTransitionService;
import org.example.callback.service.VendorCallbackDispatcher;
import org.example.callback.service.VendorCallbackKafkaPublisher;
import org.example.callback.service.VendorConfigurationResolver;
import org.example.callback.service.VendorPayloadConstructionService;
import org.example.persistence.VendorCallbackQueueConfig;
import org.example.persistence.VendorCallbackQueueConfigRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.example.callback.kafka.VendorCallbackKafkaConsumerManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/**
 * Schedules per-queue polling from {@code vendor_callback_queue_config}:
 * Kafka mode publishes rows to {@code queue_name}; direct mode dispatches HTTP from the poller thread.
 */
@Service
@ConditionalOnProperty(prefix = "app.vendor-callback", name = "enabled", havingValue = "true", matchIfMissing = true)
public class VendorCallbackPollingManager {

    private static final Logger log = LoggerFactory.getLogger(VendorCallbackPollingManager.class);

    private final VendorCallbackProperties properties;
    private final VendorCallbackQueueConfigRepository queueConfigRepository;
    private final VendorConfigurationResolver configurationResolver;
    private final VendorSourceQueueJdbcRepository sourceQueueRepository;
    private final VendorPayloadConstructionService payloadConstructionService;
    private final VendorCallbackDispatcher callbackDispatcher;
    private final VendorCallbackKafkaPublisher kafkaPublisher;
    private final QueueRowStateTransitionService stateTransitionService;

    @Autowired(required = false)
    private VendorCallbackKafkaConsumerManager kafkaConsumerManager;

    private ThreadPoolTaskScheduler taskScheduler;
    private final Map<String, ScheduledFuture<?>> scheduledTasks = new HashMap<String, ScheduledFuture<?>>();

    public VendorCallbackPollingManager(
            VendorCallbackProperties properties,
            VendorCallbackQueueConfigRepository queueConfigRepository,
            VendorConfigurationResolver configurationResolver,
            VendorSourceQueueJdbcRepository sourceQueueRepository,
            VendorPayloadConstructionService payloadConstructionService,
            VendorCallbackDispatcher callbackDispatcher,
            VendorCallbackKafkaPublisher kafkaPublisher,
            QueueRowStateTransitionService stateTransitionService) {
        this.properties = properties;
        this.queueConfigRepository = queueConfigRepository;
        this.configurationResolver = configurationResolver;
        this.sourceQueueRepository = sourceQueueRepository;
        this.payloadConstructionService = payloadConstructionService;
        this.callbackDispatcher = callbackDispatcher;
        this.kafkaPublisher = kafkaPublisher;
        this.stateTransitionService = stateTransitionService;
    }

    @PostConstruct
    public void start() {
        if (!properties.isEnabled()) {
            log.info("Vendor callback routing engine is disabled");
            return;
        }

        taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setPoolSize(properties.getSchedulerPoolSize());
        taskScheduler.setThreadNamePrefix("vendor-callback-poller-");
        taskScheduler.setWaitForTasksToCompleteOnShutdown(true);
        taskScheduler.setAwaitTerminationSeconds(60);
        taskScheduler.initialize();

        configurationResolver.refresh();
        rescheduleAll();
        log.info("Vendor callback polling manager started (dispatchViaKafka={})",
                properties.isDispatchViaKafka());
    }

    @Scheduled(fixedDelayString = "${app.vendor-callback.config-refresh-ms:60000}")
    public void refreshConfigurationsAndReschedule() {
        if (!properties.isEnabled()) {
            return;
        }
        configurationResolver.refresh();
        rescheduleAll();
        restartKafkaConsumers();
    }

    @PreDestroy
    public void shutdown() {
        cancelAllTasks();
        if (taskScheduler != null) {
            taskScheduler.shutdown();
        }
        log.info("Vendor callback polling manager stopped");
    }

    public synchronized void rescheduleAll() {
        cancelAllTasks();

        List<VendorCallbackQueueConfig> activeQueues = queueConfigRepository.findActive();
        Map<Integer, ResolvedVendorConfiguration> resolvedByQueueId = indexResolvedByQueueId();

        log.info("Active callback queues: {}", activeQueues.size());

        int scheduled = 0;
        for (VendorCallbackQueueConfig queue : activeQueues) {
            if (!StringUtils.hasText(queue.getTableName())) {
                log.warn("Skipping queue_id={} queue_name={}: table_name is not configured",
                        queue.getQueueId(), queue.getQueueName());
                continue;
            }
            if (!StringUtils.hasText(queue.getQueueName())) {
                log.warn("Skipping queue_id={}: queue_name (Kafka topic) is not configured", queue.getQueueId());
                continue;
            }

            ResolvedVendorConfiguration configuration = resolvedByQueueId.get(queue.getQueueId());
            if (configuration == null) {
                log.warn("Skipping queue_name={} table_name={} vendor={}: no resolved routing (sm_vendor_* missing?)",
                        queue.getQueueName(), queue.getTableName(), queue.getVendorName());
                continue;
            }

            Runnable task = properties.isDispatchViaKafka()
                    ? new VendorQueueKafkaPublishTask(
                            configuration,
                            sourceQueueRepository,
                            payloadConstructionService,
                            kafkaPublisher,
                            stateTransitionService)
                    : new VendorCallbackPollerTask(
                            configuration,
                            sourceQueueRepository,
                            payloadConstructionService,
                            callbackDispatcher,
                            stateTransitionService);

            long intervalMs = queue.getProducerSleepTime() != null && queue.getProducerSleepTime() > 0
                    ? queue.getProducerSleepTime()
                    : configuration.getProducerSleepTimeMs();

            ScheduledFuture<?> future = taskScheduler.scheduleWithFixedDelay(task, intervalMs);
            scheduledTasks.put(configuration.routingKey(), future);
            scheduled++;

            log.info("Scheduled {} poller queue_name={}, table_name={}, vendor={}, intervalMs={}",
                    properties.isDispatchViaKafka() ? "Kafka-publish" : "HTTP-direct",
                    queue.getQueueName(),
                    queue.getTableName(),
                    queue.getVendorName(),
                    intervalMs);
        }

        log.info("Scheduled {} callback poller(s)", scheduled);
    }

    private void restartKafkaConsumers() {
        if (properties.isDispatchViaKafka() && kafkaConsumerManager != null) {
            kafkaConsumerManager.restart();
        }
    }

    private Map<Integer, ResolvedVendorConfiguration> indexResolvedByQueueId() {
        Map<Integer, ResolvedVendorConfiguration> index = new HashMap<Integer, ResolvedVendorConfiguration>();
        for (ResolvedVendorConfiguration configuration : configurationResolver.getResolvedConfigurations()) {
            index.put(configuration.getQueueId(), configuration);
        }
        return index;
    }

    private void cancelAllTasks() {
        for (ScheduledFuture<?> future : scheduledTasks.values()) {
            future.cancel(false);
        }
        scheduledTasks.clear();
    }
}
