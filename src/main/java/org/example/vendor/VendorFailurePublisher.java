package org.example.vendor;

import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.messaging.DeadLetterEvent;
import org.example.messaging.VendorEvent;
import org.example.persistence.DeadLetterJdbcRepository;
import org.example.persistence.VendorCallbackQueueConfig;
import org.example.persistence.VendorCallbackQueueConfigRepository;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class VendorFailurePublisher {

    private final KafkaTemplate<String, VendorEvent> vendorKafkaTemplate;
    private final KafkaTemplate<String, DeadLetterEvent> deadLetterKafkaTemplate;
    private final DeadLetterJdbcRepository deadLetterRepository;
    private final VendorTopicNames topicNames;
    private final VendorCallbackQueueConfigRepository configRepository;

    public VendorFailurePublisher(
            KafkaTemplate<String, VendorEvent> vendorKafkaTemplate,
            KafkaTemplate<String, DeadLetterEvent> deadLetterKafkaTemplate,
            DeadLetterJdbcRepository deadLetterRepository,
            VendorTopicNames topicNames,
            VendorCallbackQueueConfigRepository configRepository
    ) {
        this.vendorKafkaTemplate = vendorKafkaTemplate;
        this.deadLetterKafkaTemplate = deadLetterKafkaTemplate;
        this.deadLetterRepository = deadLetterRepository;
        this.topicNames = topicNames;
        this.configRepository = configRepository;
    }

    public void publishRetry(ConsumerRecord<String, VendorEvent> record, RuntimeException exception, int retryCount) {
        VendorEvent event = record.value();
        String vendorName = vendorName(record, event);
        ProducerRecord<String, VendorEvent> retryRecord = new ProducerRecord<>(
                topicNames.retryTopic(config(vendorName)),
                record.key(),
                event
        );
        retryRecord.headers().add(VendorRetryHeaders.RETRY_COUNT, VendorRetryHeaders.retryCountBytes(retryCount));
        sendVendor(retryRecord, exception);
    }

    public void publishDeadLetter(ConsumerRecord<String, VendorEvent> record, RuntimeException exception, int retryCount) {
        VendorEvent event = record.value();
        String vendorName = vendorName(record, event);
        DeadLetterEvent deadLetterEvent = new DeadLetterEvent(
                event,
                record.topic(),
                record.partition(),
                record.offset(),
                exception.getClass().getName(),
                exception.getMessage(),
                retryCount,
                Instant.now()
        );
        deadLetterRepository.save(deadLetterEvent);
        ProducerRecord<String, DeadLetterEvent> dlqRecord = new ProducerRecord<>(
                topicNames.dlqTopic(config(vendorName)),
                record.key(),
                deadLetterEvent
        );
        sendDeadLetter(dlqRecord, exception);
    }

    public int maxRetryCount(ConsumerRecord<String, VendorEvent> record) {
        VendorCallbackQueueConfig config = config(vendorName(record, record.value()));
        if (config.getMaxRetryCount() == null) {
            return 3;
        }
        return config.getMaxRetryCount();
    }

    private VendorCallbackQueueConfig config(String vendorName) {
        VendorCallbackQueueConfig config = configRepository.findActiveByVendorName(vendorName);
        if (config == null) {
            throw new IllegalStateException("No active queue config for vendor " + vendorName);
        }
        return config;
    }

    private String vendorName(ConsumerRecord<String, VendorEvent> record, VendorEvent event) {
        String vendorFromTopic = topicNames.vendorFromTopic(record.topic());
        if (vendorFromTopic != null && !vendorFromTopic.trim().isEmpty()) {
            return vendorFromTopic;
        }
        if (event != null && event.getVendor() != null && !event.getVendor().trim().isEmpty()) {
            return event.getVendor();
        }
        throw new IllegalStateException("Unable to determine vendor for failed record from topic " + record.topic());
    }

    private void sendVendor(ProducerRecord<String, VendorEvent> record, RuntimeException originalException) {
        try {
            vendorKafkaTemplate.send(record).get(30, TimeUnit.SECONDS);
        } catch (Exception sendException) {
            originalException.addSuppressed(sendException);
            throw originalException;
        }
    }

    private void sendDeadLetter(ProducerRecord<String, DeadLetterEvent> record, RuntimeException originalException) {
        try {
            deadLetterKafkaTemplate.send(record).get(30, TimeUnit.SECONDS);
        } catch (Exception sendException) {
            originalException.addSuppressed(sendException);
            throw originalException;
        }
    }
}
