package com.syswin.temail.data.consistency.interfaces;

import com.syswin.temail.data.consistency.application.ListenerEventService;
import com.syswin.temail.data.consistency.configuration.datasource.DynamicDataSourceContextHolder;
import com.syswin.temail.data.consistency.configuration.datasource.SystemConfig;
import com.syswin.temail.data.consistency.domain.TaskApplicationEvent;
import com.zaxxer.hikari.HikariConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;

@Component
public class EventDataMonitorJob {

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

  public void eventDataMonitorJob() {
    List<HikariConfig> db = systemConfig.getDb();
    Map<String,Future<String>> resultMap = new HashMap<>();
    db.forEach(hikariConfig -> resultMap.put(hikariConfig.getPoolName(),doTask(hikariConfig.getPoolName())));
    context.publishEvent(new TaskApplicationEvent(resultMap));
  }

  @Async("checkAndSend")
  public Future<String> doTask(String dbName) {
    DynamicDataSourceContextHolder.set(dbName);
    listenerEventService.doSendingMessage();
    DynamicDataSourceContextHolder.clearDataSourceKey();
    return new AsyncResult<>("database: " + dbName + "");
  }
}
