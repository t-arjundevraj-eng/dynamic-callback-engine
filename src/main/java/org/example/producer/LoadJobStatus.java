package org.example.producer;

public class LoadJobStatus {

    private final String jobId;
    private final int producers;
    private final long targetMessages;
    private final long submittedMessages;
    private final long sentMessages;
    private final long failedMessages;
    private final boolean submissionCompleted;
    private final boolean deliveryCompleted;

    public LoadJobStatus(
            String jobId,
            int producers,
            long targetMessages,
            long submittedMessages,
            long sentMessages,
            long failedMessages,
            boolean submissionCompleted,
            boolean deliveryCompleted
    ) {
        this.jobId = jobId;
        this.producers = producers;
        this.targetMessages = targetMessages;
        this.submittedMessages = submittedMessages;
        this.sentMessages = sentMessages;
        this.failedMessages = failedMessages;
        this.submissionCompleted = submissionCompleted;
        this.deliveryCompleted = deliveryCompleted;
    }

    public String getJobId() {
        return jobId;
    }

    public int getProducers() {
        return producers;
    }

    public long getTargetMessages() {
        return targetMessages;
    }

    public long getSubmittedMessages() {
        return submittedMessages;
    }

    public long getSentMessages() {
        return sentMessages;
    }

    public long getFailedMessages() {
        return failedMessages;
    }

    public boolean isSubmissionCompleted() {
        return submissionCompleted;
    }

    public boolean isDeliveryCompleted() {
        return deliveryCompleted;
    }
}
