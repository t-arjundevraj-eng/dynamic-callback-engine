package org.example.config;

import java.util.concurrent.Executor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class ProducerPoolConfig {

    @Bean(name = "producerPool")
    Executor producerPool(AppProperties properties) {
        AppProperties.Producer producer = properties.getProducer();
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(producer.getCorePoolSize());
        executor.setMaxPoolSize(producer.getMaxPoolSize());
        executor.setQueueCapacity(producer.getQueueCapacity());
        executor.setThreadNamePrefix("producer-pool-");
        executor.initialize();
        return executor;
    }
}
