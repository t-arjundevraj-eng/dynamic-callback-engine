package org.example.vendor;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.example.messaging.VendorEvent;
import org.example.persistence.VendorCallbackQueueConfig;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class VendorEventProducerService {

    private final KafkaTemplate<String, VendorEvent> kafkaTemplate;
    private final VendorSchemaRegistry schemaRegistry;
    private final VendorTopicNames topicNames;

    public VendorEventProducerService(
            KafkaTemplate<String, VendorEvent> kafkaTemplate,
            VendorSchemaRegistry schemaRegistry,
            VendorTopicNames topicNames
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.schemaRegistry = schemaRegistry;
        this.topicNames = topicNames;
    }

    public VendorEvent send(String vendorName, Map<String, Object> fields) {
        VendorCallbackQueueConfig config = schemaRegistry.getRequired(vendorName);
        String eventId = eventId(fields);
        VendorEvent event = new VendorEvent(eventId, vendorName, "v1", fields, Instant.now());
        try {
            kafkaTemplate.send(topicNames.rawTopic(config), eventId, event).get(30, TimeUnit.SECONDS);
            return event;
        } catch (Exception ex) {
            throw new IllegalStateException("Unable to publish vendor event to Kafka", ex);
        }
    }

    private String eventId(Map<String, Object> fields) {
        Object value = fields == null ? null : fields.get("eventId");
        if (value == null && fields != null) {
            value = fields.get("messageId");
        }
        if (value == null || String.valueOf(value).trim().isEmpty()) {
            return UUID.randomUUID().toString();
        }
        return String.valueOf(value);
    }
}
