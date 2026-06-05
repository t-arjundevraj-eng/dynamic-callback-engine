package org.example.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.messaging.DeadLetterEvent;
import org.example.messaging.TelemetryEvent;
import org.example.messaging.VendorCallbackQueueMessage;
import org.example.messaging.VendorEvent;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

@Configuration
@EnableConfigurationProperties(AppProperties.class)
public class KafkaConfig {

    @Bean
    NewTopic highThroughputTopic(AppProperties properties) {
        AppProperties.Kafka kafka = properties.getKafka();
        return TopicBuilder.name(kafka.getTopic())
                .partitions(kafka.getPartitions())
                .replicas(1) // Hardcoded to 1 so it runs flawlessly on a single local development broker!
                .build();
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, TelemetryEvent> kafkaBatchListenerContainerFactory(
            KafkaProperties kafkaProperties,
            AppProperties properties
    ) {
        ConcurrentKafkaListenerContainerFactory<String, TelemetryEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        // Build our own factory on the fly using the default properties
        ConsumerFactory<String, TelemetryEvent> consumerFactory =
                new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties());

        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(properties.getKafka().getConsumerConcurrency());
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);

        return factory;
    }

    @Bean
    ConsumerFactory<String, VendorEvent> vendorEventConsumerFactory(KafkaProperties kafkaProperties) {
        java.util.Map<String, Object> props = kafkaProperties.buildConsumerProperties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, VendorEvent.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "org.example.messaging,java.util");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    ProducerFactory<String, VendorCallbackQueueMessage> vendorCallbackProducerFactory(KafkaProperties kafkaProperties) {
        java.util.Map<String, Object> props = kafkaProperties.buildProducerProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    KafkaTemplate<String, VendorCallbackQueueMessage> vendorCallbackKafkaTemplate(
            ProducerFactory<String, VendorCallbackQueueMessage> vendorCallbackProducerFactory) {
        return new KafkaTemplate<>(vendorCallbackProducerFactory);
    }
    @Bean
    ProducerFactory<String, VendorEvent> vendorEventProducerFactory(KafkaProperties kafkaProperties) {
        java.util.Map<String, Object> props = kafkaProperties.buildProducerProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    KafkaTemplate<String, VendorEvent> vendorEventKafkaTemplate(
            ProducerFactory<String, VendorEvent> vendorEventProducerFactory) {
        return new KafkaTemplate<>(vendorEventProducerFactory);
    }
    @Bean
    ProducerFactory<String, DeadLetterEvent> deadLetterProducerFactory(KafkaProperties kafkaProperties) {
        java.util.Map<String, Object> props = kafkaProperties.buildProducerProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    KafkaTemplate<String, DeadLetterEvent> deadLetterKafkaTemplate(
            ProducerFactory<String, DeadLetterEvent> deadLetterProducerFactory) {
        return new KafkaTemplate<>(deadLetterProducerFactory);
    }
    @Bean
    ProducerFactory<String, TelemetryEvent> telemetryProducerFactory(KafkaProperties kafkaProperties) {
        java.util.Map<String, Object> props = kafkaProperties.buildProducerProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    KafkaTemplate<String, TelemetryEvent> telemetryKafkaTemplate(
            ProducerFactory<String, TelemetryEvent> telemetryProducerFactory) {
        return new KafkaTemplate<>(telemetryProducerFactory);
    }

}