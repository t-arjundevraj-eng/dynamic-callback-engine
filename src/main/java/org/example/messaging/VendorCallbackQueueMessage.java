package org.example.messaging;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Kafka payload for a row polled from a vendor source queue table.
 */
public class VendorCallbackQueueMessage {

    private int queueId;
    private String queueName;
    private String sourceTableName;
    private String vendorName;
    private long rowId;
    private int retryCount;
    private Map<String, Object> row = new LinkedHashMap<String, Object>();

    public VendorCallbackQueueMessage() {
    }

    public VendorCallbackQueueMessage(
            int queueId,
            String queueName,
            String sourceTableName,
            String vendorName,
            long rowId,
            int retryCount,
            Map<String, Object> row) {
        this.queueId = queueId;
        this.queueName = queueName;
        this.sourceTableName = sourceTableName;
        this.vendorName = vendorName;
        this.rowId = rowId;
        this.retryCount = retryCount;
        this.row = row;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getVendorName() {
        return vendorName;
    }

    public void setVendorName(String vendorName) {
        this.vendorName = vendorName;
    }

    public long getRowId() {
        return rowId;
    }

    public void setRowId(long rowId) {
        this.rowId = rowId;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public Map<String, Object> getRow() {
        return row;
    }

    public void setRow(Map<String, Object> row) {
        this.row = row;
    }
}
