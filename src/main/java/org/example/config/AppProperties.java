package org.example.config;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app")
public class AppProperties {

    private final Kafka kafka = new Kafka();
    private final Producer producer = new Producer();

    public Kafka getKafka() {
        return kafka;
    }

    public Producer getProducer() {
        return producer;
    }

    public static class Kafka {
        private String topic = "high-throughput-events";
        private int partitions = 48;
        private short replicas = 1;
        private int consumerConcurrency = 12;
        private int consumerBatchSize = 500;

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public int getPartitions() {
            return partitions;
        }

        public void setPartitions(int partitions) {
            this.partitions = partitions;
        }

        public short getReplicas() {
            return replicas;
        }

        public void setReplicas(short replicas) {
            this.replicas = replicas;
        }

        public int getConsumerConcurrency() {
            return consumerConcurrency;
        }

        public void setConsumerConcurrency(int consumerConcurrency) {
            this.consumerConcurrency = consumerConcurrency;
        }

        public int getConsumerBatchSize() {
            return consumerBatchSize;
        }

        public void setConsumerBatchSize(int consumerBatchSize) {
            this.consumerBatchSize = consumerBatchSize;
        }
    }

    public static class Producer {
        private int corePoolSize = 50;
        private int maxPoolSize = 100;
        private int queueCapacity = 1000;

        public int getCorePoolSize() {
            return corePoolSize;
        }

        public void setCorePoolSize(int corePoolSize) {
            this.corePoolSize = corePoolSize;
        }

        public int getMaxPoolSize() {
            return maxPoolSize;
        }

        public void setMaxPoolSize(int maxPoolSize) {
            this.maxPoolSize = maxPoolSize;
        }

        public int getQueueCapacity() {
            return queueCapacity;
        }

        public void setQueueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
        }
    }

}
