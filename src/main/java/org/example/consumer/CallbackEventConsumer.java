package org.example.consumer;

import lombok.extern.slf4j.Slf4j;
import org.example.persistence.CallbackEventJdbcRepository;
import org.example.persistence.VendorCallbackQueueConfig;
import org.example.persistence.VendorCallbackQueueConfigRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Consumes callback queue records from Kafka and stores them in vendor-specific
 * producer tables (named with _producer suffix).
 *
 * Listens to topics configured in vendor_callback_queue_config.queue_name
 * and dynamically stores data in <table_name>_producer tables.
 */
@Component
@Slf4j
public class CallbackEventConsumer {

    private final CallbackEventJdbcRepository callbackRepository;
    private final VendorCallbackQueueConfigRepository configRepository;

    public CallbackEventConsumer(
            CallbackEventJdbcRepository callbackRepository,
            VendorCallbackQueueConfigRepository configRepository) {
        this.callbackRepository = callbackRepository;
        this.configRepository = configRepository;
    }

    /**
     * Consumes a single callback event from any vendor queue topic
     * and stores it in the corresponding <table>_producer table.
     *
     * @param records The map containing the row data from source table
     * @param topic The Kafka topic name (matches queue_name from config)
     */
    @KafkaListener(
            topics = "queue_callback_paytmchemba, queue_callback_one97, queue_callback_contest, " +
                    "queue_callback_contest_ivory, vendor_callback_mptmyanmar, queue_callback_mptmyanmar, queue_callback_one97_",
            groupId = "callback-event-consumer-group"
    )
    public void consume(
            @Payload Map<String, Object> records,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        try {
            log.debug("Received callback event from topic: {}", topic);

            // Find the config for this topic
            VendorCallbackQueueConfig config = configRepository.findActiveByQueueName(topic);
            if (config == null) {
                log.warn("No active configuration found for topic: {}. Event will be skipped.", topic);
                return;
            }

            // Save to the producer table: <table_name>_producer
            callbackRepository.save(config, records);
            log.info("Stored callback event in table: {}_producer for vendor: {}", config.getTableName(), config.getVendorName());

        } catch (Exception e) {
            log.error("Error processing callback event from topic: {}", topic, e);
            // Do not throw - allow consumer to continue
        }
    }
}

