package com.syswin.temail.data.consistency.configuration;

import static com.syswin.temail.data.consistency.mysql.stream.DataSyncFeature.POLL;

import com.syswin.temail.data.consistency.application.ListenerEventService;
import com.syswin.temail.data.consistency.application.TaskService;
import com.syswin.temail.data.consistency.application.ToggleListenerEventService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.togglz.core.manager.FeatureManager;

@Configuration
class ListenerEventServiceConfig {

  @Bean
  ListenerEventService listenerEventService(
      FeatureManager featureManager,
      TaskService taskService,
      ThreadPoolTaskExecutor executor
  ) {
    return new ToggleListenerEventService(featureManager, POLL, taskService, executor);
  }
}
