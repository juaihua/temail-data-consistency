package com.syswin.temail.data.consistency;

import com.syswin.temail.data.consistency.application.ListenerEventService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass(name = "dataSource")
public class ConsistencyAutoConfiguration {

  @Bean
  public ListenerEventService listenerEventService(){
    return new ListenerEventService();
  }
}
