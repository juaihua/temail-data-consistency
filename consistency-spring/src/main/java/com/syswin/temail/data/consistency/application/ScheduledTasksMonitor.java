package com.syswin.temail.data.consistency.application;

import com.syswin.temail.data.consistency.domain.TaskApplicationEvent;
import com.syswin.temail.data.consistency.interfaces.EventDataMonitorJob;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.scheduling.annotation.Scheduled;

public class ScheduledTasksMonitor implements ApplicationListener<TaskApplicationEvent> {

  private static final Logger logger = LoggerFactory.getLogger(ScheduledTasksMonitor.class);

  private final EventDataMonitorJob eventDataMonitorJob;

  Map<String,Future<String>> result = null;

  @Autowired
  public ScheduledTasksMonitor(EventDataMonitorJob eventDataMonitorJob) {
    this.eventDataMonitorJob = eventDataMonitorJob;
  }

  @Scheduled(fixedRate = 1000)
  public void restartJobAndReport() {
    if (result != null) {
      logger.debug(result.toString());
      result.forEach((key,future) -> {
        if (future.isDone()) {
          try {
            future.get();
          } catch (Exception e) {
            eventDataMonitorJob.doTask(key);
            logger.error("EXCEPTION->" + e.getMessage());
          }
          result.remove(key);
        }
      });
    }
    logger.info("current time : " + new SimpleDateFormat("HH:mm:ss").format(new Date()));
  }

  @Override
  public void onApplicationEvent(TaskApplicationEvent taskApplicationEvent) {
    result = (Map<String,Future<String>>)taskApplicationEvent.getSource();
  }
}
