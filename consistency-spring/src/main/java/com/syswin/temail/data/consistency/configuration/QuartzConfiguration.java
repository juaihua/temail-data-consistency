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

  private static final String monitorDataCron = "* * * * * ?";

  @Bean
  public JobDetail eventDataMonitorTaskDetail() {
    return JobBuilder.newJob(EventDataMonitorJob.class).withIdentity("eventDataMonitorTaskDetail").storeDurably().build();
  }

  @Bean
  public Trigger eventDataMonitorTaskTrigger() {
    CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule(monitorDataCron);
    return TriggerBuilder.newTrigger().forJob(eventDataMonitorTaskDetail())
        .withIdentity("eventDataMonitorTaskTrigger")
        .withSchedule(scheduleBuilder)
        .build();
  }


}
