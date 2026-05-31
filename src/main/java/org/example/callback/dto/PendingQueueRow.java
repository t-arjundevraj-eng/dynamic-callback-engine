package org.example.callback.dto;

import java.util.Map;

/**
 * A polled source row with resolved identity and retry metadata for state transitions.
 */
public class PendingQueueRow {

    private final long rowId;
    private final int retryCount;
    private final Map<String, Object> fields;

    public PendingQueueRow(long rowId, int retryCount, Map<String, Object> fields) {
        this.rowId = rowId;
        this.retryCount = retryCount;
        this.fields = fields;
    }

    public long getRowId() {
        return rowId;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public Map<String, Object> getFields() {
        return fields;
    }
}
