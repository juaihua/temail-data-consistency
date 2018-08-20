package com.syswin.temail.data.consistency.configuration;

import com.syswin.temail.data.consistency.interfaces.EventDataMonitorJob;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class QuartzConfiguration {

  @Bean
  public JobDetail eventDataMonitorJob(){
    return JobBuilder.newJob(EventDataMonitorJob.class)
        .withIdentity("eventDataMonitorJob")
        .storeDurably()
        .build();
  }
  @Bean
  public Trigger taskTrigger() {
    CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule("*/1 * * * * ?");
    return TriggerBuilder.newTrigger()
        .forJob(eventDataMonitorJob())
        .withIdentity("eventDataMonitorJob")
        .withSchedule(scheduleBuilder)
        .build();
  }
}
