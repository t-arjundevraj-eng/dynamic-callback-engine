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
import org.example.callback.service.VendorCallbackDispatcher;
import org.example.callback.service.VendorPayloadConstructionService;
import org.example.callback.util.QueueRowMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Polls unprocessed source rows, dispatches callbacks asynchronously in batch, then bulk-updates row state.
 */
public class VendorCallbackPollerTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(VendorCallbackPollerTask.class);

    private final ResolvedVendorConfiguration configuration;
    private final VendorSourceQueueJdbcRepository sourceQueueRepository;
    private final VendorPayloadConstructionService payloadConstructionService;
    private final VendorCallbackDispatcher callbackDispatcher;
    private final QueueRowStateTransitionService stateTransitionService;

    public VendorCallbackPollerTask(
            ResolvedVendorConfiguration configuration,
            VendorSourceQueueJdbcRepository sourceQueueRepository,
            VendorPayloadConstructionService payloadConstructionService,
            VendorCallbackDispatcher callbackDispatcher,
            QueueRowStateTransitionService stateTransitionService) {
        this.configuration = configuration;
        this.sourceQueueRepository = sourceQueueRepository;
        this.payloadConstructionService = payloadConstructionService;
        this.callbackDispatcher = callbackDispatcher;
        this.stateTransitionService = stateTransitionService;
    }

    @Override
    public void run() {
        try {
            pollValidateAndDispatch();
        } catch (Exception ex) {
            log.error("Polling cycle failed for vendor={}, circle={}, table={}. Will retry on next interval.",
                    configuration.getVendorName(),
                    configuration.getCircle(),
                    configuration.getSourceTableName(),
                    ex);
        }
    }

    private void pollValidateAndDispatch() {
        List<Map<String, Object>> rows = sourceQueueRepository.pollUnprocessedRows(
                configuration.getSourceTableName(),
                configuration.getFetchSize());

        if (rows.isEmpty()) {
            log.debug("No unprocessed rows in source table {} for vendor {}",
                    configuration.getSourceTableName(), configuration.getVendorName());
            return;
        }

        log.info("Polled {} unprocessed row(s) from {} for vendor={}, circle={}",
                rows.size(),
                configuration.getSourceTableName(),
                configuration.getVendorName(),
                configuration.getCircle());

        List<QueueRowStateUpdate> stateUpdates = new ArrayList<QueueRowStateUpdate>();
        List<CompletableFuture<QueueRowStateUpdate>> asyncStateFutures = new ArrayList<CompletableFuture<QueueRowStateUpdate>>();

        int validationDlq = 0;

        for (Map<String, Object> row : rows) {
            PendingQueueRow pendingRow;
            try {
                pendingRow = QueueRowMetadata.fromRow(row);
            } catch (IllegalArgumentException ex) {
                log.warn("Skipping row without id in table {}: {}", configuration.getSourceTableName(), ex.getMessage());
                continue;
            }

            RawQueueEvent event = new RawQueueEvent(configuration.getSourceTableName(), pendingRow.getFields());
            Optional<Map<String, Object>> payload =
                    payloadConstructionService.buildPayload(event, configuration);

            if (!payload.isPresent()) {
                stateUpdates.add(stateTransitionService.validationFailure(
                        pendingRow.getRowId(), pendingRow.getRetryCount()));
                validationDlq++;
                continue;
            }

            final String rowId = pendingRow.getRowId();
            final int retryCount = pendingRow.getRetryCount();
            final int maxRetryCount = configuration.getMaxRetryCount();

            CompletableFuture<QueueRowStateUpdate> stateFuture = callbackDispatcher
                    .dispatchAsync(configuration, payload.get())
                    .thenApply(result -> stateTransitionService.onDispatchResult(
                            rowId, retryCount, maxRetryCount, result))
                    .exceptionally(ex -> {
                        log.error("Unexpected async dispatch error for rowId={} vendor={}: {}",
                                rowId, configuration.getVendorName(), ex.getMessage());
                        return stateTransitionService.onDispatchResult(
                                rowId,
                                retryCount,
                                maxRetryCount,
                                DispatchResult.failure(ex.getMessage()));
                    });

            asyncStateFutures.add(stateFuture);
        }

        if (!asyncStateFutures.isEmpty()) {
            CompletableFuture.allOf(asyncStateFutures.toArray(new CompletableFuture[0])).join();
            for (CompletableFuture<QueueRowStateUpdate> future : asyncStateFutures) {
                stateUpdates.add(future.join());
            }
        }

        if (!stateUpdates.isEmpty()) {
            sourceQueueRepository.bulkUpdateRowStates(configuration.getSourceTableName(), stateUpdates);
        }

        log.info("Callback cycle complete vendor={}, circle={}: asyncDispatched={}, validationDlq={}, stateUpdates={}",
                configuration.getVendorName(),
                configuration.getCircle(),
                asyncStateFutures.size(),
                validationDlq,
                stateUpdates.size());
    }
}
