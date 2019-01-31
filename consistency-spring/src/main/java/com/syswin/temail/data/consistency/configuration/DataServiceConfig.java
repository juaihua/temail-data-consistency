package com.syswin.temail.data.consistency.configuration;

import static com.syswin.temail.data.consistency.mysql.stream.DataSyncFeature.POLL;

import com.syswin.temail.data.consistency.application.HandleEventDataService;
import com.syswin.temail.data.consistency.application.MQProducer;
import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import com.syswin.temail.data.consistency.mysql.stream.ToggleMqProducer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.togglz.core.manager.FeatureManager;

@Configuration
class DataServiceConfig {

  @Bean
  HandleEventDataService eventDataService(
      FeatureManager featureManager,
      MQProducer mqProducer,
      ListenerEventRepo listenerEventRepo) {

    return new HandleEventDataService(
        new ToggleMqProducer(featureManager, POLL, mqProducer),
        listenerEventRepo);
  }
}
