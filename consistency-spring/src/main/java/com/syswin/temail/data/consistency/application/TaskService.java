package com.syswin.temail.data.consistency.application;

import com.syswin.temail.data.consistency.configuration.datasource.DynamicDataSourceContextHolder;
import com.syswin.temail.data.consistency.domain.ListenerEvent;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

@Service
public class TaskService {

  private static final Logger logger = LoggerFactory.getLogger(TaskService.class);

  private final ThreadPoolTaskExecutor taskExecutor;

  private final HandleEventDataService dataService;

  @Autowired
  public TaskService(ThreadPoolTaskExecutor taskExecutor, HandleEventDataService dataService) {
    this.taskExecutor = taskExecutor;
    this.dataService = dataService;
  }

  public Map<String, List<ListenerEvent>> findToBeSend(String dbName) {
    DynamicDataSourceContextHolder.set(dbName);
    Map<String, List<ListenerEvent>> toBeSend = dataService.findToBeSend();
    DynamicDataSourceContextHolder.clearDataSourceKey();
    return toBeSend;
  }

  public void doSendingMessage(String dbName) {
    while (true) {
    logger.debug("doSendingMessage");
      Map<String, List<ListenerEvent>> sendMap = findToBeSend(dbName);
      if (sendMap.isEmpty()) {
        try {
          logger.debug("dbName:{},no data,thread sleeping", dbName);
          Thread.sleep(1000);
          continue;
        } catch (InterruptedException e) {
          logger.warn("error,thread is being interrupted!");
        }
      }
      sendInLoop(dbName, sendMap);
    }
  }

  public void sendInLoop(String dbName, Map<String, List<ListenerEvent>> sendMap) {
    LinkedList<Future<?>> results = new LinkedList<>();
    sendMap.forEach((k, v) -> {
      Future<?> result = taskExecutor.submit(() -> {
        v.forEach(
            x -> {
              DynamicDataSourceContextHolder.set(dbName);
              logger.debug("doTask-in-executer->" + dbName);
              dataService.sendAndUpdate(x);
              logger.debug("RocketMQ send data=>[{}]", x);
              DynamicDataSourceContextHolder.clearDataSourceKey();
            }
        );
      });
      results.add(result);
    });
    checkTask(results);
  }

  private void checkTask(LinkedList<Future<?>> results) {
    while (true) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        logger.warn("error,thread is being interrupted!");
      }
      boolean flag = false;
      for (Future<?> result : results) {
        if (!result.isDone()) {
          continue;
        }
        flag = result.isDone();
      }
      if (flag) {
        break;
      }
    }
  }
}
