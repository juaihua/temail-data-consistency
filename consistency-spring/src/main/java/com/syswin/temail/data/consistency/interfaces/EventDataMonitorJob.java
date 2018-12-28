package com.syswin.temail.data.consistency.interfaces;

import com.syswin.temail.data.consistency.application.ListenerEventService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.util.CollectionUtils;

@DisallowConcurrentExecution
@Slf4j
public class EventDataMonitorJob extends QuartzJobBean {

  @Autowired
  private ListenerEventService listenerEventService;

  @Value(value = "${app.slice.topics:all}")
  private String[] topicArray;

  @Override
  protected void executeInternal(JobExecutionContext jobExecutionContext) throws JobExecutionException {
    List<String> topics = CollectionUtils.arrayToList(topicArray);
    log.info("topics:{} task started", topics);
    listenerEventService.doTask(topics);
  }
}
