package org.example.vendor;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.config.AppProperties;
import org.example.consumer.VendorIngestionConsumer;
import org.example.messaging.VendorEvent;
import org.example.persistence.VendorCallbackQueueConfig;
import org.example.persistence.VendorCallbackQueueConfigRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

@Component
public class DynamicVendorKafkaListenerManager implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(DynamicVendorKafkaListenerManager.class);

    private final VendorCallbackQueueConfigRepository configRepository;
    private final VendorTopicNames topicNames;
    private final VendorIngestionConsumer ingestionConsumer;
    private final KafkaProperties kafkaProperties;
    private final KafkaAdmin kafkaAdmin;
    private final AppProperties appProperties;
    private final Map<Integer, ConcurrentMessageListenerContainer<String, VendorEvent>> containers =
            new LinkedHashMap<>();
    private volatile boolean running;

    public DynamicVendorKafkaListenerManager(
            VendorCallbackQueueConfigRepository configRepository,
            VendorTopicNames topicNames,
            VendorIngestionConsumer ingestionConsumer,
            KafkaProperties kafkaProperties,
            KafkaAdmin kafkaAdmin,
            AppProperties appProperties
    ) {
        this.configRepository = configRepository;
        this.topicNames = topicNames;
        this.ingestionConsumer = ingestionConsumer;
        this.kafkaProperties = kafkaProperties;
        this.kafkaAdmin = kafkaAdmin;
        this.appProperties = appProperties;
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        List<VendorCallbackQueueConfig> activeConfigs = configRepository.findActive();
        for (VendorCallbackQueueConfig config : activeConfigs) {
            createTopics(config);
            ConcurrentMessageListenerContainer<String, VendorEvent> container = container(config);
            container.start();
            containers.put(config.getQueueId(), container);
            log.info("Started vendor Kafka container vendor={} topics=[{}, {}] concurrency={} fetchSize={}",
                    config.getVendorName(),
                    topicNames.rawTopic(config),
                    topicNames.retryTopic(config),
                    concurrency(config),
                    fetchSize(config));
        }
        running = true;
    }

    private void createTopics(VendorCallbackQueueConfig config) {
        AppProperties.Kafka kafka = appProperties.getKafka();
        NewTopic rawTopic = TopicBuilder.name(topicNames.rawTopic(config))
                .partitions(kafka.getPartitions())
                .replicas(kafka.getReplicas())
                .build();
        NewTopic retryTopic = TopicBuilder.name(topicNames.retryTopic(config))
                .partitions(kafka.getPartitions())
                .replicas(kafka.getReplicas())
                .build();
        NewTopic dlqTopic = TopicBuilder.name(topicNames.dlqTopic(config))
                .partitions(kafka.getPartitions())
                .replicas(kafka.getReplicas())
                .build();
        kafkaAdmin.createOrModifyTopics(rawTopic, retryTopic, dlqTopic);
    }

    @Override
    public void stop() {
        for (ConcurrentMessageListenerContainer<String, VendorEvent> container : containers.values()) {
            container.stop();
        }
        containers.clear();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    private ConcurrentMessageListenerContainer<String, VendorEvent> container(VendorCallbackQueueConfig config) {
        ContainerProperties containerProperties = new ContainerProperties(
                topicNames.rawTopic(config),
                topicNames.retryTopic(config)
        );
        containerProperties.setGroupId(groupId(config));
        containerProperties.setAckMode(ContainerProperties.AckMode.RECORD);
        containerProperties.setMessageListener((MessageListener<String, VendorEvent>) ingestionConsumer::consume);

        ConcurrentMessageListenerContainer<String, VendorEvent> container =
                new ConcurrentMessageListenerContainer<>(consumerFactory(config), containerProperties);
        container.setConcurrency(concurrency(config));
        container.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1000L, 2L)));
        return container;
    }

    private DefaultKafkaConsumerFactory<String, VendorEvent> consumerFactory(VendorCallbackQueueConfig config) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, VendorEvent.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "org.example.messaging,java.util");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, fetchSize(config));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId(config));
        return new DefaultKafkaConsumerFactory<>(props);
    }

    private String groupId(VendorCallbackQueueConfig config) {
        String baseGroupId = kafkaProperties.getConsumer().getGroupId();
        if (baseGroupId == null || baseGroupId.trim().isEmpty()) {
            baseGroupId = "vendor-ingestion";
        }
        return baseGroupId + "-" + config.getQueueName();
    }

    private int concurrency(VendorCallbackQueueConfig config) {
        if (config.getConsPoolSize() == null || config.getConsPoolSize() < 1) {
            return 1;
        }
        return config.getConsPoolSize();
    }

    private int fetchSize(VendorCallbackQueueConfig config) {
        if (config.getFetchSize() == null || config.getFetchSize() < 1) {
            return 500;
        }
        return config.getFetchSize();
    }
}
