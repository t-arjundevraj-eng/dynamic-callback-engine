package org.example.vendor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.example.persistence.VendorCallbackQueueConfig;
import org.example.persistence.VendorCallbackQueueConfigRepository;
import org.springframework.stereotype.Component;

@Component
public class VendorTopicNames {

    /** REST/Kafka ingestion topics (separate from callback dispatch topics which use {@code queue_name} as-is). */
    private static final String INGEST_SUFFIX = ".ingest";
    private static final String RETRY_SUFFIX = ".retry";
    private static final String DLQ_SUFFIX = ".dlq";

    private final VendorCallbackQueueConfigRepository configRepository;

    public VendorTopicNames(VendorCallbackQueueConfigRepository configRepository) {
        this.configRepository = configRepository;
    }

    public String rawTopic(String vendor) {
        VendorCallbackQueueConfig config = configRepository.findActiveByVendorName(vendor);
        if (config == null) {
            throw new VendorValidationException("Unknown or inactive vendor: " + vendor);
        }
        return rawTopic(config);
    }

    public String rawTopic(VendorCallbackQueueConfig config) {
        return config.getQueueName() + INGEST_SUFFIX;
    }

    /** Kafka topic for DB row callback dispatch (matches {@code vendor_callback_queue_config.queue_name}). */
    public String callbackDispatchTopic(VendorCallbackQueueConfig config) {
        return config.getQueueName();
    }

    public String retryTopic(String vendor) {
        return rawTopic(vendor) + RETRY_SUFFIX;
    }

    public String retryTopic(VendorCallbackQueueConfig config) {
        return rawTopic(config) + RETRY_SUFFIX;
    }

    public String dlqTopic(String vendor) {
        return rawTopic(vendor) + DLQ_SUFFIX;
    }

    public String dlqTopic(VendorCallbackQueueConfig config) {
        return rawTopic(config) + DLQ_SUFFIX;
    }

    public String vendorFromTopic(String topic) {
        for (VendorCallbackQueueConfig config : configRepository.findActive()) {
            if (rawTopic(config).equals(topic)
                    || retryTopic(config).equals(topic)
                    || dlqTopic(config).equals(topic)
                    || callbackDispatchTopic(config).equals(topic)) {
                return config.getVendorName();
            }
        }
        return null;
    }

    public String[] consumerTopics() {
        List<String> topics = new ArrayList<>();
        for (VendorCallbackQueueConfig config : configRepository.findActive()) {
            topics.add(rawTopic(config));
            topics.add(retryTopic(config));
        }
        return topics.toArray(new String[0]);
    }

    public List<String> allTopics() {
        List<String> topics = new ArrayList<>();
        for (VendorCallbackQueueConfig config : configRepository.findActive()) {
            topics.add(rawTopic(config));
            topics.add(retryTopic(config));
            topics.add(dlqTopic(config));
        }
        return Collections.unmodifiableList(topics);
    }
}
