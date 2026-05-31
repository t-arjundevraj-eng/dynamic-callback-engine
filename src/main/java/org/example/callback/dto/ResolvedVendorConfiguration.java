package org.example.callback.dto;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Fully validated routing rule for one vendor + circle + queue binding.
 */
public class ResolvedVendorConfiguration {

    private final int vendorId;
    private final String vendorName;
    private final String circle;
    private final String callbackUrl;
    private final String channelUrl;
    private final CallbackHttpMethod httpMethod;
    private final int queueId;
    private final String queueName;
    private final String sourceTableName;
    private final int fetchSize;
    private final long producerSleepTimeMs;
    private final int maxRetryCount;
    private final Set<String> allowedOperatorIds;
    private final Set<String> allowedPackIds;
    private final Set<String> allowedIpAddresses;
    private final List<VendorParamDefinition> paramDefinitions;

    public ResolvedVendorConfiguration(
            int vendorId,
            String vendorName,
            String circle,
            String callbackUrl,
            String channelUrl,
            CallbackHttpMethod httpMethod,
            int queueId,
            String queueName,
            String sourceTableName,
            int fetchSize,
            long producerSleepTimeMs,
            int maxRetryCount,
            Set<String> allowedOperatorIds,
            Set<String> allowedPackIds,
            Set<String> allowedIpAddresses,
            List<VendorParamDefinition> paramDefinitions) {
        this.vendorId = vendorId;
        this.vendorName = vendorName;
        this.circle = circle;
        this.callbackUrl = callbackUrl;
        this.channelUrl = channelUrl;
        this.httpMethod = httpMethod;
        this.queueId = queueId;
        this.queueName = queueName;
        this.sourceTableName = sourceTableName;
        this.fetchSize = fetchSize;
        this.producerSleepTimeMs = producerSleepTimeMs;
        this.maxRetryCount = maxRetryCount;
        this.allowedOperatorIds = Collections.unmodifiableSet(new LinkedHashSet<>(allowedOperatorIds));
        this.allowedPackIds = Collections.unmodifiableSet(new LinkedHashSet<>(allowedPackIds));
        this.allowedIpAddresses = Collections.unmodifiableSet(new LinkedHashSet<>(allowedIpAddresses));
        this.paramDefinitions = Collections.unmodifiableList(paramDefinitions);
    }

    public int getVendorId() {
        return vendorId;
    }

    public String getVendorName() {
        return vendorName;
    }

    public String getCircle() {
        return circle;
    }

    public String getCallbackUrl() {
        return callbackUrl;
    }

    public String getChannelUrl() {
        return channelUrl;
    }

    public CallbackHttpMethod getHttpMethod() {
        return httpMethod;
    }

    public int getQueueId() {
        return queueId;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public long getProducerSleepTimeMs() {
        return producerSleepTimeMs;
    }

    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    public Set<String> getAllowedOperatorIds() {
        return allowedOperatorIds;
    }

    public Set<String> getAllowedPackIds() {
        return allowedPackIds;
    }

    public Set<String> getAllowedIpAddresses() {
        return allowedIpAddresses;
    }

    public List<VendorParamDefinition> getParamDefinitions() {
        return paramDefinitions;
    }

    public String routingKey() {
        return vendorId + ":" + circle + ":" + queueId;
    }
}
