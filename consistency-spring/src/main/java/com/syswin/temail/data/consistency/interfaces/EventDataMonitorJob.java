package com.syswin.temail.data.consistency.interfaces;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

public class EventDataMonitorJob extends QuartzJobBean{

  public static boolean flag = false;

  @Override
  protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
    flag = true;
  }

}
