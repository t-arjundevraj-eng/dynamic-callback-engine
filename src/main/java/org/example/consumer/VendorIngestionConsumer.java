package org.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.messaging.VendorEvent;
import org.example.persistence.VendorCallbackQueueConfig;
import org.example.persistence.VendorEventJdbcRepository;
import org.example.vendor.VendorEventValidator;
import org.example.vendor.VendorFailurePublisher;
import org.example.vendor.VendorRetryHeaders;
import org.example.vendor.VendorValidationException;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

@Component
public class VendorIngestionConsumer {

    private final VendorEventValidator validator;
    private final VendorEventJdbcRepository repository;
    private final VendorFailurePublisher failurePublisher;

    public VendorIngestionConsumer(
            VendorEventValidator validator,
            VendorEventJdbcRepository repository,
            VendorFailurePublisher failurePublisher
    ) {
        this.validator = validator;
        this.repository = repository;
        this.failurePublisher = failurePublisher;
    }

    public void consume(ConsumerRecord<String, VendorEvent> record) {
        try {
            VendorEvent event = record.value();
            VendorCallbackQueueConfig config = validator.validate(event);
            repository.save(config, event);
        } catch (VendorValidationException | IllegalArgumentException ex) {
            failurePublisher.publishDeadLetter(record, ex, VendorRetryHeaders.retryCount(record));
        } catch (DataAccessException ex) {
            handleRetryableFailure(record, ex);
        }
    }

    private void handleRetryableFailure(ConsumerRecord<String, VendorEvent> record, RuntimeException ex) {
        int nextRetryCount = VendorRetryHeaders.retryCount(record) + 1;
        int maxRetryCount = failurePublisher.maxRetryCount(record);
        if (nextRetryCount > maxRetryCount) {
            failurePublisher.publishDeadLetter(record, ex, nextRetryCount - 1);
            return;
        }
        failurePublisher.publishRetry(record, ex, nextRetryCount);
    }
}
