package com.syswin.temail.data.consistency.configuration;

import com.syswin.temail.data.consistency.application.HandleEventDataService;
import com.syswin.temail.data.consistency.application.MQProducer;
import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class DataServiceConfig {

  @Bean
  HandleEventDataService eventDataService(
      MQProducer mqProducer,
      ListenerEventRepo listenerEventRepo) {

    return new HandleEventDataService(
        mqProducer,
        listenerEventRepo);
  }
}
