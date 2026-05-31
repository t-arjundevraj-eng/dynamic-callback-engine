package org.example.callback.config;

import java.util.concurrent.Executor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.client.RestTemplate;

@Configuration
@EnableScheduling
@EnableConfigurationProperties(VendorCallbackProperties.class)
public class VendorCallbackRestTemplateConfig {

    @Bean(name = "vendorCallbackRestTemplate")
    public RestTemplate vendorCallbackRestTemplate(
            RestTemplateBuilder builder,
            VendorCallbackProperties properties) {
        return builder
                .requestFactory(() -> {
                    SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
                    factory.setConnectTimeout(properties.getConnectTimeoutMs());
                    factory.setReadTimeout(properties.getReadTimeoutMs());
                    return factory;
                })
                .build();
    }

    @Bean(name = "vendorCallbackDispatchExecutor")
    public Executor vendorCallbackDispatchExecutor(VendorCallbackProperties properties) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(properties.getDispatchCorePoolSize());
        executor.setMaxPoolSize(properties.getDispatchMaxPoolSize());
        executor.setQueueCapacity(properties.getDispatchQueueCapacity());
        executor.setThreadNamePrefix("vendor-callback-http-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        executor.initialize();
        return executor;
    }
}
