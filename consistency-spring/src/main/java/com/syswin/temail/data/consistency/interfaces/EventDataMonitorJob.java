package com.syswin.temail.data.consistency.interfaces;

import com.syswin.temail.data.consistency.application.ListenerEventService;
import com.syswin.temail.data.consistency.domain.TaskApplicationEvent;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class EventDataMonitorJob {


  private static final Logger logger = LoggerFactory.getLogger(EventDataMonitorJob.class);

  private final ListenerEventService listenerEventService;

  private final ApplicationContext context;

  @Value("#{'${topics}'.split(',')}")
  private List<String> topics;

  @Autowired
  public EventDataMonitorJob(ListenerEventService listenerEventService, ApplicationContext context) {
    this.listenerEventService = listenerEventService;
    this.context = context;
  }

  public void eventDataMonitorJob(){
    if(topics == null || topics.size() == 0){
      topics = Arrays.asList("all");
    }
    Map<String,Future<String>> resultMap = new HashMap<>();
    topics.forEach(topic -> {
      logger.info("db:{} task started",topic);
      resultMap.put(topic, listenerEventService.doTask(topic));
    });
    context.publishEvent(new TaskApplicationEvent(resultMap));
  }

}
