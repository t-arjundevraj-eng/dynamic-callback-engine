package org.example.callback.service;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.example.callback.dto.DispatchResult;
import org.example.callback.dto.ResolvedVendorConfiguration;
import org.example.messaging.VendorCallbackQueueMessage;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
public class VendorCallbackKafkaPublisher {

    private final KafkaTemplate<String, VendorCallbackQueueMessage> kafkaTemplate;
    private final Executor publishExecutor;

    public VendorCallbackKafkaPublisher(
            KafkaTemplate<String, VendorCallbackQueueMessage> vendorCallbackKafkaTemplate,
            @Qualifier("vendorCallbackDispatchExecutor") Executor publishExecutor) {
        this.kafkaTemplate = vendorCallbackKafkaTemplate;
        this.publishExecutor = publishExecutor;
    }

    public CompletableFuture<DispatchResult> publishAsync(
            ResolvedVendorConfiguration configuration,
            String rowId,
            int retryCount,
            Map<String, Object> row) {
        VendorCallbackQueueMessage message = new VendorCallbackQueueMessage(
                configuration.getQueueId(),
                configuration.getQueueName(),
                configuration.getSourceTableName(),
                configuration.getVendorName(),
                rowId,
                retryCount,
                row);

        String topic = configuration.getQueueName();
        String key = String.valueOf(rowId);

        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        SendResult<String, VendorCallbackQueueMessage> result =
                                kafkaTemplate.send(topic, key, message).get();
                        return DispatchResult.publishSuccess();
                    } catch (Exception ex) {
                        return DispatchResult.failure(ex.getMessage());
                    }
                },
                publishExecutor);
    }
}
