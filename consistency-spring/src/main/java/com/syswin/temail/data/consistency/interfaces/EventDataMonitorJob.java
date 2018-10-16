package com.syswin.temail.data.consistency.interfaces;

import com.syswin.temail.data.consistency.application.ListenerEventService;
import com.syswin.temail.data.consistency.application.ScheduledTasksMonitor;
import com.syswin.temail.data.consistency.configuration.datasource.DynamicDataSourceContextHolder;
import com.syswin.temail.data.consistency.configuration.datasource.SystemConfig;
import com.syswin.temail.data.consistency.domain.TaskApplicationEvent;
import com.zaxxer.hikari.HikariConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;

@Component
public class EventDataMonitorJob {


  private static final Logger logger = LoggerFactory.getLogger(EventDataMonitorJob.class);

  private final ListenerEventService listenerEventService;

  private final SystemConfig systemConfig;

  private final ApplicationContext context;

  @Autowired
  public EventDataMonitorJob(ListenerEventService listenerEventService,
      SystemConfig systemConfig, ApplicationContext context) {
    this.listenerEventService = listenerEventService;
    this.systemConfig = systemConfig;
    this.context = context;
  }

  public void eventDataMonitorJob(){
    List<HikariConfig> db = systemConfig.getDb();
    Map<String,Future<String>> resultMap = new HashMap<>();
    db.forEach(hikariConfig -> {
      logger.debug("db:{} task started",hikariConfig.getPoolName());
      resultMap.put(hikariConfig.getPoolName(), listenerEventService.doTask(hikariConfig.getPoolName()));
    });
    context.publishEvent(new TaskApplicationEvent(resultMap));
  }

}
