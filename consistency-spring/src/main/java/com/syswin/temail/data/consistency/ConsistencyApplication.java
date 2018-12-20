package com.syswin.temail.data.consistency;

import com.syswin.temail.data.consistency.interfaces.EventDataMonitorJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ConsistencyApplication implements CommandLineRunner{

  @Autowired
  private EventDataMonitorJob eventDataMonitorJob;

  public static void main(String[] args) {
    SpringApplication.run(ConsistencyApplication.class,args);
  }


  @Override
  public void run(String... args) throws Exception {
//    eventDataMonitorJob.eventDataMonitorJob();
  }
}
