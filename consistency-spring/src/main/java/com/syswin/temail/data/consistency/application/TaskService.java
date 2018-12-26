package com.syswin.temail.data.consistency.application;

import com.syswin.temail.data.consistency.domain.ListenerEvent;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TaskService {


  private final ThreadPoolTaskExecutor taskExecutor;

  private final HandleEventDataService dataService;

  @Autowired
  public TaskService(ThreadPoolTaskExecutor taskExecutor, HandleEventDataService dataService) {
    this.taskExecutor = taskExecutor;
    this.dataService = dataService;
  }

  public Map<String, List<ListenerEvent>> findToBeSend(String topic) {
    Map<String, List<ListenerEvent>> toBeSend = dataService.findToBeSend(topic);
    return toBeSend;
  }

  public void doSendingMessage(String topic) {

    while (true) {
    log.debug("doSendingMessage");
      Map<String, List<ListenerEvent>> sendMap = findToBeSend(topic);
      if (sendMap.isEmpty()) {
        try {
          log.debug("topic:{},no data,thread sleeping", topic);
          Thread.sleep(100);
          continue;
        } catch (InterruptedException e) {
          log.warn("error,thread is being interrupted!");
        }
      }
      sendInLoop(topic, sendMap);
    }
  }

  public void sendInLoop(String topic, Map<String, List<ListenerEvent>> sendMap) {
    LinkedList<Future<?>> results = new LinkedList<>();
    sendMap.forEach((k, v) -> {
      Future<?> result = taskExecutor.submit(() -> {
        v.forEach(
            x -> {
              log.debug("doTask-in-executer->" + topic);
              dataService.sendAndUpdate(x);
              log.debug("RocketMQ send data=>[{}]", x);
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
        log.warn("error,thread is being interrupted!");
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
