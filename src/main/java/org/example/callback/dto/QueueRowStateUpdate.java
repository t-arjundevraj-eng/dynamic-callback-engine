package org.example.callback.dto;

public class QueueRowStateUpdate {

    private final long rowId;
    private final String processStatus;
    private final int retryCount;

    public QueueRowStateUpdate(long rowId, String processStatus, int retryCount) {
        this.rowId = rowId;
        this.processStatus = processStatus;
        this.retryCount = retryCount;
    }

    public long getRowId() {
        return rowId;
    }

    public String getProcessStatus() {
        return processStatus;
    }

    public int getRetryCount() {
        return retryCount;
    }
}
