package com.syswin.temail.data.consistency.configuration;
import com.syswin.temail.data.consistency.interfaces.FlushHistoryDataJob;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class QuartzConfiguration {

  @Value("${app.consistency.flush.data.cron}")
  private String cron;

  @Bean
  public JobDetail pushUnreadCountTaskDetail() {
    return JobBuilder.newJob(FlushHistoryDataJob.class).withIdentity("flushHistoryDataJobDetail").storeDurably().build();
  }

  @Bean
  public Trigger uploadTaskTrigger() {
    CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule(cron);
    return TriggerBuilder.newTrigger().forJob(pushUnreadCountTaskDetail())
        .withIdentity("flushHistoryDataJobTrigger")
        .withSchedule(scheduleBuilder)
        .build();
  }

}
