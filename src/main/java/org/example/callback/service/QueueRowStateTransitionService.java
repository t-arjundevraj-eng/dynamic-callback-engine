package org.example.callback.service;

import org.example.callback.dto.DispatchResult;
import org.example.callback.dto.ProcessStatus;
import org.example.callback.dto.QueueRowStateUpdate;
import org.springframework.stereotype.Service;

@Service
public class QueueRowStateTransitionService {

    public QueueRowStateUpdate onDispatchResult(long rowId, int currentRetryCount, int maxRetryCount, DispatchResult result) {
        if (result.isSuccess()) {
            return new QueueRowStateUpdate(rowId, ProcessStatus.COMPLETED, currentRetryCount);
        }
        return onFailure(rowId, currentRetryCount, maxRetryCount);
    }

    public QueueRowStateUpdate onKafkaPublishResult(long rowId, int currentRetryCount, int maxRetryCount, DispatchResult result) {
        if (result.isSuccess()) {
            return new QueueRowStateUpdate(rowId, ProcessStatus.PUBLISHED, currentRetryCount);
        }
        return onFailure(rowId, currentRetryCount, maxRetryCount);
    }

    private QueueRowStateUpdate onFailure(long rowId, int currentRetryCount, int maxRetryCount) {
        int nextRetryCount = currentRetryCount + 1;
        if (nextRetryCount < maxRetryCount) {
            return new QueueRowStateUpdate(rowId, ProcessStatus.RETRY, nextRetryCount);
        }
        return new QueueRowStateUpdate(rowId, ProcessStatus.DLQ, nextRetryCount);
    }

    public QueueRowStateUpdate validationFailure(long rowId, int currentRetryCount) {
        return new QueueRowStateUpdate(rowId, ProcessStatus.DLQ, currentRetryCount);
    }
}
