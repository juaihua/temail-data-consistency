package com.syswin.temail.data.consistency.interfaces;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles("test")
public class EventDataMonitorJobTest {

  @Autowired
  private Scheduler scheduler;

  @Autowired
  private SchedulerFactoryBean schedulerFactoryBean;

  @Test
  public void startEnvironment() throws Exception {
    assertThat(scheduler).isNotNull();
    assertThat(schedulerFactoryBean).isNotNull();
    System.out.println(scheduler.getJobDetail(JobKey.jobKey("eventDataMonitorJob")));
    assertThat(scheduler.getJobDetail(JobKey.jobKey("eventDataMonitorJob")).getKey().toString())
        .isEqualTo("DEFAULT.eventDataMonitorJob");
  }
}