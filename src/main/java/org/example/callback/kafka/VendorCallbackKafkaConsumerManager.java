package org.example.callback.kafka;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.callback.config.VendorCallbackProperties;
import org.example.callback.consumer.VendorCallbackKafkaConsumer;
import org.example.config.AppProperties;
import org.example.messaging.VendorCallbackQueueMessage;
import org.example.persistence.VendorCallbackQueueConfig;
import org.example.persistence.VendorCallbackQueueConfigRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Starts Kafka consumers for each active {@code vendor_callback_queue_config} row,
 * listening on {@code queue_name} and dispatching HTTP callbacks.
 */
@Component
@ConditionalOnProperty(prefix = "app.vendor-callback", name = "dispatch-via-kafka", havingValue = "true", matchIfMissing = true)
public class VendorCallbackKafkaConsumerManager implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(VendorCallbackKafkaConsumerManager.class);

    private final VendorCallbackQueueConfigRepository queueConfigRepository;
    private final VendorCallbackKafkaConsumer callbackConsumer;
    private final KafkaProperties kafkaProperties;
    private final KafkaAdmin kafkaAdmin;
    private final AppProperties appProperties;
    private final VendorCallbackProperties callbackProperties;

    private final Map<Integer, ConcurrentMessageListenerContainer<String, VendorCallbackQueueMessage>> containers =
            new LinkedHashMap<Integer, ConcurrentMessageListenerContainer<String, VendorCallbackQueueMessage>>();
    private volatile boolean running;

    public VendorCallbackKafkaConsumerManager(
            VendorCallbackQueueConfigRepository queueConfigRepository,
            VendorCallbackKafkaConsumer callbackConsumer,
            KafkaProperties kafkaProperties,
            KafkaAdmin kafkaAdmin,
            AppProperties appProperties,
            VendorCallbackProperties callbackProperties) {
        this.queueConfigRepository = queueConfigRepository;
        this.callbackConsumer = callbackConsumer;
        this.kafkaProperties = kafkaProperties;
        this.kafkaAdmin = kafkaAdmin;
        this.appProperties = appProperties;
        this.callbackProperties = callbackProperties;
    }

    @Override
    public void start() {
        if (running || !callbackProperties.isEnabled()) {
            return;
        }
        List<VendorCallbackQueueConfig> activeQueues = queueConfigRepository.findActive();
        for (VendorCallbackQueueConfig queue : activeQueues) {
            if (queue.getQueueName() == null || queue.getQueueName().trim().isEmpty()) {
                continue;
            }
            createTopic(queue);
            ConcurrentMessageListenerContainer<String, VendorCallbackQueueMessage> container = buildContainer(queue);
            container.start();
            containers.put(queue.getQueueId(), container);
            log.info("Started callback Kafka consumer queue_name={} table_name={} vendor={} concurrency={}",
                    queue.getQueueName(),
                    queue.getTableName(),
                    queue.getVendorName(),
                    concurrency(queue));
        }
        running = true;
    }

    @Override
    public void stop() {
        for (ConcurrentMessageListenerContainer<String, VendorCallbackQueueMessage> container : containers.values()) {
            container.stop();
        }
        containers.clear();
        running = false;
    }

    public synchronized void restart() {
        if (!callbackProperties.isEnabled() || !callbackProperties.isDispatchViaKafka()) {
            return;
        }
        stop();
        start();
        log.info("Restarted callback Kafka consumers after configuration refresh");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 1;
    }

    private void createTopic(VendorCallbackQueueConfig queue) {
        int partitions = Math.max(1, callbackProperties.getCallbackTopicPartitions());
        NewTopic topic = TopicBuilder.name(queue.getQueueName())
                .partitions(partitions)
                .replicas(appProperties.getKafka().getReplicas())
                .build();
        kafkaAdmin.createOrModifyTopics(topic);
    }

    private ConcurrentMessageListenerContainer<String, VendorCallbackQueueMessage> buildContainer(
            VendorCallbackQueueConfig queue) {
        ContainerProperties properties = new ContainerProperties(queue.getQueueName());
        properties.setGroupId(callbackConsumerGroupId(queue));
        properties.setAckMode(ContainerProperties.AckMode.RECORD);
        properties.setMessageListener(
                (MessageListener<String, VendorCallbackQueueMessage>) callbackConsumer::consume);

        ConcurrentMessageListenerContainer<String, VendorCallbackQueueMessage> container =
                new ConcurrentMessageListenerContainer<>(consumerFactory(queue), properties);
        container.setConcurrency(concurrency(queue));
        container.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1000L, 2L)));
        return container;
    }

    private DefaultKafkaConsumerFactory<String, VendorCallbackQueueMessage> consumerFactory(
            VendorCallbackQueueConfig queue) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, VendorCallbackQueueMessage.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "org.example.messaging,java.util");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, fetchSize(queue));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, callbackConsumerGroupId(queue));
        return new DefaultKafkaConsumerFactory<String, VendorCallbackQueueMessage>(props);
    }

    private String callbackConsumerGroupId(VendorCallbackQueueConfig queue) {
        return "vendor-callback-" + queue.getQueueName();
    }

    private int concurrency(VendorCallbackQueueConfig queue) {
        int maxPartitions = Math.max(1, callbackProperties.getCallbackTopicPartitions());
        int requested = queue.getConsPoolSize() != null && queue.getConsPoolSize() > 0
                ? queue.getConsPoolSize()
                : 1;
        return Math.min(requested, maxPartitions);
    }

    private int fetchSize(VendorCallbackQueueConfig queue) {
        if (queue.getFetchSize() != null && queue.getFetchSize() > 0) {
            return queue.getFetchSize();
        }
        return 50;
    }
}
