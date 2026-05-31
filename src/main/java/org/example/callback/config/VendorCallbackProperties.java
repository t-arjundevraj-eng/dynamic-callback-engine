package org.example.callback.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.vendor-callback")
public class VendorCallbackProperties {

    private boolean enabled = true;
    private boolean dispatchViaKafka = true;
    private int connectTimeoutMs = 5_000;
    private int readTimeoutMs = 30_000;
    private int schedulerPoolSize = 20;
    private long configRefreshMs = 60_000;
    private long defaultProducerSleepMs = 1_000;
    private int dispatchCorePoolSize = 50;
    private int dispatchMaxPoolSize = 100;
    private int dispatchQueueCapacity = 1_000;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isDispatchViaKafka() {
        return dispatchViaKafka;
    }

    public void setDispatchViaKafka(boolean dispatchViaKafka) {
        this.dispatchViaKafka = dispatchViaKafka;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public void setConnectTimeoutMs(int connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
    }

    public int getReadTimeoutMs() {
        return readTimeoutMs;
    }

    public void setReadTimeoutMs(int readTimeoutMs) {
        this.readTimeoutMs = readTimeoutMs;
    }

    public int getSchedulerPoolSize() {
        return schedulerPoolSize;
    }

    public void setSchedulerPoolSize(int schedulerPoolSize) {
        this.schedulerPoolSize = schedulerPoolSize;
    }

    public long getConfigRefreshMs() {
        return configRefreshMs;
    }

    public void setConfigRefreshMs(long configRefreshMs) {
        this.configRefreshMs = configRefreshMs;
    }

    public long getDefaultProducerSleepMs() {
        return defaultProducerSleepMs;
    }

    public void setDefaultProducerSleepMs(long defaultProducerSleepMs) {
        this.defaultProducerSleepMs = defaultProducerSleepMs;
    }

    public int getDispatchCorePoolSize() {
        return dispatchCorePoolSize;
    }

    public void setDispatchCorePoolSize(int dispatchCorePoolSize) {
        this.dispatchCorePoolSize = dispatchCorePoolSize;
    }

    public int getDispatchMaxPoolSize() {
        return dispatchMaxPoolSize;
    }

    public void setDispatchMaxPoolSize(int dispatchMaxPoolSize) {
        this.dispatchMaxPoolSize = dispatchMaxPoolSize;
    }

    public int getDispatchQueueCapacity() {
        return dispatchQueueCapacity;
    }

    public void setDispatchQueueCapacity(int dispatchQueueCapacity) {
        this.dispatchQueueCapacity = dispatchQueueCapacity;
    }
}
