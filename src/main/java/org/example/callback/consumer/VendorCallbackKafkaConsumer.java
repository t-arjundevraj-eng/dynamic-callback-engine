package org.example.callback.consumer;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.callback.dto.DispatchResult;
import org.example.callback.dto.RawQueueEvent;
import org.example.callback.dto.ResolvedVendorConfiguration;
import org.example.callback.repository.VendorSourceQueueJdbcRepository;
import org.example.callback.service.QueueRowStateTransitionService;
import org.example.callback.service.VendorCallbackDispatcher;
import org.example.callback.service.VendorConfigurationResolver;
import org.example.callback.service.VendorPayloadConstructionService;
import org.example.messaging.VendorCallbackQueueMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class VendorCallbackKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(VendorCallbackKafkaConsumer.class);

    private final VendorConfigurationResolver configurationResolver;
    private final VendorPayloadConstructionService payloadConstructionService;
    private final VendorCallbackDispatcher callbackDispatcher;
    private final VendorSourceQueueJdbcRepository sourceQueueRepository;
    private final QueueRowStateTransitionService stateTransitionService;

    public VendorCallbackKafkaConsumer(
            VendorConfigurationResolver configurationResolver,
            VendorPayloadConstructionService payloadConstructionService,
            VendorCallbackDispatcher callbackDispatcher,
            VendorSourceQueueJdbcRepository sourceQueueRepository,
            QueueRowStateTransitionService stateTransitionService) {
        this.configurationResolver = configurationResolver;
        this.payloadConstructionService = payloadConstructionService;
        this.callbackDispatcher = callbackDispatcher;
        this.sourceQueueRepository = sourceQueueRepository;
        this.stateTransitionService = stateTransitionService;
    }

    public void consume(ConsumerRecord<String, VendorCallbackQueueMessage> record) {
        VendorCallbackQueueMessage message = record.value();
        if (message == null) {
            log.warn("Null callback message on topic {}", record.topic());
            return;
        }

        ResolvedVendorConfiguration configuration = resolveConfiguration(message.getQueueId());
        if (configuration == null) {
            log.warn("No resolved configuration for queueId={} topic={}", message.getQueueId(), record.topic());
            return;
        }

        RawQueueEvent event = new RawQueueEvent(message.getSourceTableName(), message.getRow());
        Optional<Map<String, Object>> payload = payloadConstructionService.buildPayload(event, configuration);
        if (!payload.isPresent()) {
            sourceQueueRepository.bulkUpdateRowStates(
                    message.getSourceTableName(),
                    Collections.singletonList(
                            stateTransitionService.validationFailure(message.getRowId(), message.getRetryCount())));
            return;
        }

        try {
            DispatchResult result = callbackDispatcher
                    .dispatchAsync(configuration, payload.get())
                    .get();
            sourceQueueRepository.bulkUpdateRowStates(
                    message.getSourceTableName(),
                    Collections.singletonList(
                            stateTransitionService.onDispatchResult(
                                    message.getRowId(),
                                    message.getRetryCount(),
                                    configuration.getMaxRetryCount(),
                                    result)));
            log.info("Processed Kafka callback message rowId={} topic={} success={}",
                    message.getRowId(), record.topic(), result.isSuccess());
        } catch (Exception ex) {
            log.error("Failed processing callback message rowId={} topic={}: {}",
                    message.getRowId(), record.topic(), ex.getMessage());
            sourceQueueRepository.bulkUpdateRowStates(
                    message.getSourceTableName(),
                    Collections.singletonList(
                            stateTransitionService.onDispatchResult(
                                    message.getRowId(),
                                    message.getRetryCount(),
                                    configuration.getMaxRetryCount(),
                                    DispatchResult.failure(ex.getMessage()))));
        }
    }

    private ResolvedVendorConfiguration resolveConfiguration(int queueId) {
        for (ResolvedVendorConfiguration configuration : configurationResolver.getResolvedConfigurations()) {
            if (configuration.getQueueId() == queueId) {
                return configuration;
            }
        }
        return null;
    }
}
