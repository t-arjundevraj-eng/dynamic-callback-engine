package org.example.producer;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import org.example.config.AppProperties;
import org.example.messaging.TelemetryEvent;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

@Service
public class LoadProducerService {

    private final KafkaTemplate<String, TelemetryEvent> kafkaTemplate;
    private final Executor producerPool;
    private final AppProperties properties;
    private final Map<String, LoadJob> jobs = new ConcurrentHashMap<>();

    public LoadProducerService(
            KafkaTemplate<String, TelemetryEvent> kafkaTemplate,
            @Qualifier("producerPool") Executor producerPool,
            AppProperties properties
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.producerPool = producerPool;
        this.properties = properties;
    }

    public LoadJobStatus start(LoadRequest request) {
        String jobId = UUID.randomUUID().toString();
        long targetMessages = request.getMessagesPerProducer() * request.getProducers();
        LoadJob job = new LoadJob(jobId, request.getProducers(), targetMessages);
        jobs.put(jobId, job);

        for (int producerId = 1; producerId <= request.getProducers(); producerId++) {
            int currentProducerId = producerId;
            producerPool.execute(() -> runProducer(job, currentProducerId, request));
        }

        return job.status();
    }

    public LoadJobStatus status(String jobId) {
        LoadJob job = jobs.get(jobId);
        if (job == null) {
            throw new IllegalArgumentException("Unknown job id: " + jobId);
        }
        return job.status();
    }

    private void runProducer(LoadJob job, int producerId, LoadRequest request) {
        String payload = payload(request.getPayloadBytes());
        try {
            for (long sequence = 1; sequence <= request.getMessagesPerProducer(); sequence++) {
                String eventId = producerId + "-" + sequence + "-" + UUID.randomUUID();
                TelemetryEvent event = new TelemetryEvent(eventId, producerId, sequence, payload, Instant.now());
                job.submitted();
                ListenableFuture<?> future = kafkaTemplate.send(
                        properties.getKafka().getTopic(),
                        String.valueOf(producerId),
                        event
                );
                future.addCallback(result -> job.sent(), error -> job.failed());
            }
        } finally {
            job.producerDone();
        }
    }

    private String payload(int payloadBytes) {
        char[] chars = new char[payloadBytes];
        Arrays.fill(chars, 'x');
        return new String(chars);
    }
}
