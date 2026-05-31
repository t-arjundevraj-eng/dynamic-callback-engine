package org.example.messaging;

import java.time.Instant;

public class TelemetryEvent {

    private String eventId;
    private int producerId;
    private long sequenceNumber;
    private String payload;
    private Instant generatedAt;

    public TelemetryEvent() {
    }

    public TelemetryEvent(String eventId, int producerId, long sequenceNumber, String payload, Instant generatedAt) {
        this.eventId = eventId;
        this.producerId = producerId;
        this.sequenceNumber = sequenceNumber;
        this.payload = payload;
        this.generatedAt = generatedAt;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public int getProducerId() {
        return producerId;
    }

    public void setProducerId(int producerId) {
        this.producerId = producerId;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public Instant getGeneratedAt() {
        return generatedAt;
    }

    public void setGeneratedAt(Instant generatedAt) {
        this.generatedAt = generatedAt;
    }
}
