package com.syswin.temail.data.consistency.interfaces;

import com.syswin.temail.data.consistency.application.ListenerEventService;
import com.syswin.temail.data.consistency.application.MQProducer;
import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import com.syswin.temail.data.consistency.domain.SendingStatus;
import com.syswin.temail.data.consistency.utils.JsonConverter;
import java.util.List;
import java.util.Map;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
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
