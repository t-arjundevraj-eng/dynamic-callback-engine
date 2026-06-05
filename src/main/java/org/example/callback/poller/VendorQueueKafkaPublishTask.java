package org.example.callback.poller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.example.callback.dto.DispatchResult;
import org.example.callback.dto.PendingQueueRow;
import org.example.callback.dto.QueueRowStateUpdate;
import org.example.callback.dto.RawQueueEvent;
import org.example.callback.dto.ResolvedVendorConfiguration;
import org.example.callback.repository.VendorSourceQueueJdbcRepository;
import org.example.callback.service.QueueRowStateTransitionService;
import org.example.callback.service.VendorCallbackKafkaPublisher;
import org.example.callback.service.VendorPayloadConstructionService;
import org.example.callback.util.QueueRowMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Polls unprocessed rows from the configured source table and publishes them to the Kafka topic
 * named in {@code vendor_callback_queue_config.queue_name}.
 */
public class VendorQueueKafkaPublishTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(VendorQueueKafkaPublishTask.class);

    private final ResolvedVendorConfiguration configuration;
    private final VendorSourceQueueJdbcRepository sourceQueueRepository;
    private final VendorPayloadConstructionService payloadConstructionService;
    private final VendorCallbackKafkaPublisher kafkaPublisher;
    private final QueueRowStateTransitionService stateTransitionService;

    public VendorQueueKafkaPublishTask(
            ResolvedVendorConfiguration configuration,
            VendorSourceQueueJdbcRepository sourceQueueRepository,
            VendorPayloadConstructionService payloadConstructionService,
            VendorCallbackKafkaPublisher kafkaPublisher,
            QueueRowStateTransitionService stateTransitionService) {
        this.configuration = configuration;
        this.sourceQueueRepository = sourceQueueRepository;
        this.payloadConstructionService = payloadConstructionService;
        this.kafkaPublisher = kafkaPublisher;
        this.stateTransitionService = stateTransitionService;
    }

    @Override
    public void run() {
        try {
            pollAndPublish();
        } catch (Exception ex) {
            log.error("Kafka publish cycle failed for vendor={}, queue={}, table={}.",
                    configuration.getVendorName(),
                    configuration.getQueueName(),
                    configuration.getSourceTableName(),
                    ex);
        }
    }

    private void pollAndPublish() {
        List<Map<String, Object>> rows = sourceQueueRepository.pollUnprocessedRows(
                configuration.getSourceTableName(),
                configuration.getFetchSize());

        if (rows.isEmpty()) {
            log.debug("No unprocessed rows in {} for queue {}",
                    configuration.getSourceTableName(), configuration.getQueueName());
            return;
        }

        log.info("Publishing {} row(s) from {} to Kafka topic {} (vendor={})",
                rows.size(),
                configuration.getSourceTableName(),
                configuration.getQueueName(),
                configuration.getVendorName());

        List<QueueRowStateUpdate> stateUpdates = new ArrayList<QueueRowStateUpdate>();
        List<CompletableFuture<QueueRowStateUpdate>> publishFutures = new ArrayList<CompletableFuture<QueueRowStateUpdate>>();

        int validationDlq = 0;
        int maxRetryCount = configuration.getMaxRetryCount();

        for (Map<String, Object> row : rows) {
            PendingQueueRow pendingRow;
            try {
                pendingRow = QueueRowMetadata.fromRow(row);
            } catch (IllegalArgumentException ex) {
                log.warn("Skipping row without id in table {}: {}", configuration.getSourceTableName(), ex.getMessage());
                continue;
            }

            RawQueueEvent event = new RawQueueEvent(configuration.getSourceTableName(), pendingRow.getFields());
            Optional<Map<String, Object>> payload = payloadConstructionService.buildPayload(event, configuration);
            if (!payload.isPresent()) {
                stateUpdates.add(stateTransitionService.validationFailure(
                        pendingRow.getRowId(), pendingRow.getRetryCount()));
                validationDlq++;
                continue;
            }

            final String rowId = pendingRow.getRowId();
            final int retryCount = pendingRow.getRetryCount();

            CompletableFuture<QueueRowStateUpdate> stateFuture = kafkaPublisher
                    .publishAsync(configuration, rowId, retryCount, pendingRow.getFields())
                    .thenApply(result -> stateTransitionService.onKafkaPublishResult(
                            rowId, retryCount, maxRetryCount, result))
                    .exceptionally(ex -> {
                        log.error("Unexpected Kafka publish error rowId={} topic={}: {}",
                                rowId, configuration.getQueueName(), ex.getMessage());
                        return stateTransitionService.onKafkaPublishResult(
                                rowId, retryCount, maxRetryCount, DispatchResult.failure(ex.getMessage()));
                    });

            publishFutures.add(stateFuture);
        }

        if (!publishFutures.isEmpty()) {
            CompletableFuture.allOf(publishFutures.toArray(new CompletableFuture[0])).join();
            for (CompletableFuture<QueueRowStateUpdate> future : publishFutures) {
                stateUpdates.add(future.join());
            }
        }

        if (!stateUpdates.isEmpty()) {
            sourceQueueRepository.bulkUpdateRowStates(configuration.getSourceTableName(), stateUpdates);
        }

        log.info("Kafka publish cycle complete topic={}, published={}, validationDlq={}, stateUpdates={}",
                configuration.getQueueName(),
                publishFutures.size(),
                validationDlq,
                stateUpdates.size());
    }
}
