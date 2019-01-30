package com.syswin.temail.data.consistency.infrastructure;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {
    "com.syswin.temail.data.consistency.configuration",
    "com.syswin.temail.data.consistency.infrastructure"
})
public class DbTestApplication {

  public static void main(String[] args) {
    SpringApplication.run(DbTestApplication.class, args);
  }
}
