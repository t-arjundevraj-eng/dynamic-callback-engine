package org.example.messaging;

import java.time.Instant;

public class DeadLetterEvent {

    private VendorEvent originalEvent;
    private String sourceTopic;
    private Integer sourcePartition;
    private Long sourceOffset;
    private String errorType;
    private String errorMessage;
    private Integer retryCount;
    private Instant failedAt;

    public DeadLetterEvent() {
    }

    public DeadLetterEvent(
            VendorEvent originalEvent,
            String sourceTopic,
            Integer sourcePartition,
            Long sourceOffset,
            String errorType,
            String errorMessage,
            Integer retryCount,
            Instant failedAt
    ) {
        this.originalEvent = originalEvent;
        this.sourceTopic = sourceTopic;
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
        this.errorType = errorType;
        this.errorMessage = errorMessage;
        this.retryCount = retryCount;
        this.failedAt = failedAt;
    }

    public VendorEvent getOriginalEvent() {
        return originalEvent;
    }

    public void setOriginalEvent(VendorEvent originalEvent) {
        this.originalEvent = originalEvent;
    }

    public String getSourceTopic() {
        return sourceTopic;
    }

    public void setSourceTopic(String sourceTopic) {
        this.sourceTopic = sourceTopic;
    }

    public Integer getSourcePartition() {
        return sourcePartition;
    }

    public void setSourcePartition(Integer sourcePartition) {
        this.sourcePartition = sourcePartition;
    }

    public Long getSourceOffset() {
        return sourceOffset;
    }

    public void setSourceOffset(Long sourceOffset) {
        this.sourceOffset = sourceOffset;
    }

    public String getErrorType() {
        return errorType;
    }

    public void setErrorType(String errorType) {
        this.errorType = errorType;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public Integer getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(Integer retryCount) {
        this.retryCount = retryCount;
    }

    public Instant getFailedAt() {
        return failedAt;
    }

    public void setFailedAt(Instant failedAt) {
        this.failedAt = failedAt;
    }
}
