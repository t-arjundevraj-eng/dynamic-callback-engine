package org.example.producer;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

public class LoadRequest {

    @Min(1)
    @Max(100)
    private int producers = 50;

    @Min(1)
    private long messagesPerProducer = 100_000;

    @Min(1)
    @Max(10_000)
    private int payloadBytes = 256;

    public int getProducers() {
        return producers;
    }

    public void setProducers(int producers) {
        this.producers = producers;
    }

    public long getMessagesPerProducer() {
        return messagesPerProducer;
    }

    public void setMessagesPerProducer(long messagesPerProducer) {
        this.messagesPerProducer = messagesPerProducer;
    }

    public int getPayloadBytes() {
        return payloadBytes;
    }

    public void setPayloadBytes(int payloadBytes) {
        this.payloadBytes = payloadBytes;
    }
}
