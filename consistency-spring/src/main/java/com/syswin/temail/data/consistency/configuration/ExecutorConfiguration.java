package com.syswin.temail.data.consistency.configuration;

import java.util.concurrent.ThreadPoolExecutor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableAsync
public class ExecutorConfiguration {

  @Value("${app.consistency.executor.core-pool-size}")
  private int corePoolSize;
  @Value("${app.consistency.executor.keep-alive-seconds}")
  private int keepAliveSeconds;
  @Value("${app.consistency.executor.max-pool-size}")
  private int maxPoolSize;
  @Value("${app.consistency.executor.queue-capacity}")
  private int queueCapacity;

  @Bean
  public ThreadPoolTaskExecutor taskExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(corePoolSize);
    executor.setKeepAliveSeconds(keepAliveSeconds);
    executor.setMaxPoolSize(maxPoolSize);
    executor.setQueueCapacity(queueCapacity);
    executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    return executor;
  }
}
