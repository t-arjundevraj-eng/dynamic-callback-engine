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
import org.example.callback.service.VendorConfigurationResolver;
import org.example.callback.service.VendorPayloadConstructionService;
import org.example.persistence.VendorCallbackQueueConfig;
import org.example.persistence.VendorCallbackQueueConfigRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;

/**
 * Database-to-HTTP gateway orchestrator: schedules one polling task per resolved vendor route
 * for all active entries in {@code vendor_callback_queue_config}.
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
    private final QueueRowStateTransitionService stateTransitionService;

    private ThreadPoolTaskScheduler taskScheduler;
    private final Map<String, ScheduledFuture<?>> scheduledTasks = new HashMap<String, ScheduledFuture<?>>();

    public VendorCallbackPollingManager(
            VendorCallbackProperties properties,
            VendorCallbackQueueConfigRepository queueConfigRepository,
            VendorConfigurationResolver configurationResolver,
            VendorSourceQueueJdbcRepository sourceQueueRepository,
            VendorPayloadConstructionService payloadConstructionService,
            VendorCallbackDispatcher callbackDispatcher,
            QueueRowStateTransitionService stateTransitionService) {
        this.properties = properties;
        this.queueConfigRepository = queueConfigRepository;
        this.configurationResolver = configurationResolver;
        this.sourceQueueRepository = sourceQueueRepository;
        this.payloadConstructionService = payloadConstructionService;
        this.callbackDispatcher = callbackDispatcher;
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

        List<VendorCallbackQueueConfig> activeQueues = queueConfigRepository.findActive();
        log.info("Found {} active vendor callback queue configuration(s)", activeQueues.size());

        configurationResolver.refresh();
        rescheduleAll();
        log.info("Vendor callback polling manager started");
    }

    @Scheduled(fixedDelayString = "${app.vendor-callback.config-refresh-ms:60000}")
    public void refreshConfigurationsAndReschedule() {
        if (!properties.isEnabled()) {
            return;
        }
        configurationResolver.refresh();
        rescheduleAll();
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

        List<ResolvedVendorConfiguration> configurations = configurationResolver.getResolvedConfigurations();
        for (ResolvedVendorConfiguration configuration : configurations) {
            VendorCallbackPollerTask task = new VendorCallbackPollerTask(
                    configuration,
                    sourceQueueRepository,
                    payloadConstructionService,
                    callbackDispatcher,
                    stateTransitionService);

            ScheduledFuture<?> future = taskScheduler.scheduleWithFixedDelay(
                    task,
                    configuration.getProducerSleepTimeMs());

            scheduledTasks.put(configuration.routingKey(), future);
            log.info("Scheduled HTTP callback poller vendor={}, circle={}, table={}, intervalMs={}",
                    configuration.getVendorName(),
                    configuration.getCircle(),
                    configuration.getSourceTableName(),
                    configuration.getProducerSleepTimeMs());
        }
    }

    private void cancelAllTasks() {
        for (ScheduledFuture<?> future : scheduledTasks.values()) {
            future.cancel(false);
        }
        scheduledTasks.clear();
    }
}
