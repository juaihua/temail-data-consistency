package com.syswin.temail.data.consistency;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@PropertySource("classpath:toggle.properties")
public class ConsistencyApplication implements CommandLineRunner{

  public static void main(String[] args) {
    SpringApplication.run(ConsistencyApplication.class,args);
  }

  @Override
  public void run(String... args) throws Exception {
  }
}
