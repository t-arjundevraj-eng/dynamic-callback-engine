package org.example.persistence;

public class VendorCallbackQueueConfig {

    private Integer queueId;
    private String queueName;
    private Integer consPoolSize;
    private Integer prodBlockQueueSize;
    private Integer consBlockQueueSize;
    private Integer fetchSize;
    private Long producerSleepTime;
    private Long consumerSleepTime;
    private Boolean active;
    private Boolean vendorCircleFlag;
    private String vendorName;
    private String circleName;
    private Integer maxRetryCount;
    private String tableName;

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public Integer getConsPoolSize() {
        return consPoolSize;
    }

    public void setConsPoolSize(Integer consPoolSize) {
        this.consPoolSize = consPoolSize;
    }

    public Integer getProdBlockQueueSize() {
        return prodBlockQueueSize;
    }

    public void setProdBlockQueueSize(Integer prodBlockQueueSize) {
        this.prodBlockQueueSize = prodBlockQueueSize;
    }

    public Integer getConsBlockQueueSize() {
        return consBlockQueueSize;
    }

    public void setConsBlockQueueSize(Integer consBlockQueueSize) {
        this.consBlockQueueSize = consBlockQueueSize;
    }

    public Integer getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(Integer fetchSize) {
        this.fetchSize = fetchSize;
    }

    public Long getProducerSleepTime() {
        return producerSleepTime;
    }

    public void setProducerSleepTime(Long producerSleepTime) {
        this.producerSleepTime = producerSleepTime;
    }

    public Long getConsumerSleepTime() {
        return consumerSleepTime;
    }

    public void setConsumerSleepTime(Long consumerSleepTime) {
        this.consumerSleepTime = consumerSleepTime;
    }

    public Boolean getActive() {
        return active;
    }

    public void setActive(Boolean active) {
        this.active = active;
    }

    public Boolean getVendorCircleFlag() {
        return vendorCircleFlag;
    }

    public void setVendorCircleFlag(Boolean vendorCircleFlag) {
        this.vendorCircleFlag = vendorCircleFlag;
    }

    public String getVendorName() {
        return vendorName;
    }

    public void setVendorName(String vendorName) {
        this.vendorName = vendorName;
    }

    public String getCircleName() {
        return circleName;
    }

    public void setCircleName(String circleName) {
        this.circleName = circleName;
    }

    public Integer getMaxRetryCount() {
        return maxRetryCount;
    }

    public void setMaxRetryCount(Integer maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
