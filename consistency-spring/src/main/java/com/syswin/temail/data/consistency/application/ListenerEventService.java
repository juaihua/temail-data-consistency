package com.syswin.temail.data.consistency.application;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ListenerEventService {


  private final TaskService taskService;

  private final ThreadPoolTaskExecutor taskExecutor;

  @Autowired
  public ListenerEventService(TaskService taskService, ThreadPoolTaskExecutor taskExecutor) {
    this.taskService = taskService;
    this.taskExecutor = taskExecutor;
  }

  public void doTask(List<String> topics){
    List<Future<?>> results = new LinkedList<>();
    for (String x : topics) {
      Future<?> future = taskExecutor.submit(() -> taskService.doSendingMessage(x));
      results.add(future);
    }
    for (Future future : results) {
      try {
        future.get(5, TimeUnit.SECONDS);
      } catch (Exception e) {
        log.error("task execute error:{}", e);
      }
    }
  }
}
