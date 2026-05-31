package org.example.consumer;

import java.util.List;
import org.example.messaging.TelemetryEvent;
import org.example.persistence.ConsumedEventJdbcRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TelemetryConsumer {

    private final ConsumedEventJdbcRepository repository;

    public TelemetryConsumer(ConsumedEventJdbcRepository repository) {
        this.repository = repository;
    }

    @Transactional
    @KafkaListener(
            topics = "${app.kafka.topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaBatchListenerContainerFactory"
    )
    public void consume(List<TelemetryEvent> events) {
        repository.saveBatch(events);
    }
}
