package org.example.messaging;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

public class VendorEvent {

    private String eventId;
    private String vendor;
    private String schemaVersion;
    private Map<String, Object> fields = new LinkedHashMap<>();
    private Instant receivedAt;

    public VendorEvent() {
    }

    public VendorEvent(String eventId, String vendor, String schemaVersion, Map<String, Object> fields, Instant receivedAt) {
        this.eventId = eventId;
        this.vendor = vendor;
        this.schemaVersion = schemaVersion;
        this.fields = fields;
        this.receivedAt = receivedAt;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getVendor() {
        return vendor;
    }

    public void setVendor(String vendor) {
        this.vendor = vendor;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(String schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public void setFields(Map<String, Object> fields) {
        this.fields = fields;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }

    public void setReceivedAt(Instant receivedAt) {
        this.receivedAt = receivedAt;
    }
}
