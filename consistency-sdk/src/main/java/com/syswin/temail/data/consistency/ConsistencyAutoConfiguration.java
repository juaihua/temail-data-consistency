package com.syswin.temail.data.consistency;

import com.syswin.temail.data.consistency.application.TemailMqSender;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConsistencyAutoConfiguration {

  @Bean
  public TemailMqSender temailMqSender(){
    return new TemailMqSender();
  }
}
