package org.example.producer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

class LoadJob {

    private final String jobId;
    private final int producers;
    private final long targetMessages;
    private final CountDownLatch producersDone;
    private final AtomicLong submittedMessages = new AtomicLong();
    private final AtomicLong sentMessages = new AtomicLong();
    private final AtomicLong failedMessages = new AtomicLong();

    LoadJob(String jobId, int producers, long targetMessages) {
        this.jobId = jobId;
        this.producers = producers;
        this.targetMessages = targetMessages;
        this.producersDone = new CountDownLatch(producers);
    }

    void producerDone() {
        producersDone.countDown();
    }

    void sent() {
        sentMessages.incrementAndGet();
    }

    void submitted() {
        submittedMessages.incrementAndGet();
    }

    void failed() {
        failedMessages.incrementAndGet();
    }

    LoadJobStatus status() {
        long sent = sentMessages.get();
        long failed = failedMessages.get();
        return new LoadJobStatus(
                jobId,
                producers,
                targetMessages,
                submittedMessages.get(),
                sent,
                failed,
                producersDone.getCount() == 0,
                sent + failed >= targetMessages
        );
    }
}
