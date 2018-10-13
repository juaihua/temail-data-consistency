package com.syswin.temail.data.consistency.interfaces;

import com.syswin.temail.data.consistency.application.ListenerEventService;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;

@DisallowConcurrentExecution
public class EventDataMonitorJob extends QuartzJobBean {

  private final ListenerEventService listenerEventService;

  @Autowired
  public EventDataMonitorJob(ListenerEventService listenerEventService) {
    this.listenerEventService = listenerEventService;
  }

  @Override
  protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
    listenerEventService.doSendingMessage();
  }
}
