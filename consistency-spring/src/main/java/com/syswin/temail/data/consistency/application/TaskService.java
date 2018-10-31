package com.syswin.temail.data.consistency.application;

import com.syswin.temail.data.consistency.configuration.datasource.DynamicDataSourceContextHolder;
import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import com.syswin.temail.data.consistency.domain.SendingStatus;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

@Service
public class TaskService {

  private static final Logger logger = LoggerFactory.getLogger(TaskService.class);

  private final ThreadPoolTaskExecutor taskExecutor;

  private final MQProducer mqProducer;

  private final ListenerEventRepo listenerEventRepo;

  @Autowired
  public TaskService(ThreadPoolTaskExecutor taskExecutor, MQProducer mqProducer,
      ListenerEventRepo listenerEventRepo) {
    this.taskExecutor = taskExecutor;
    this.mqProducer = mqProducer;
    this.listenerEventRepo = listenerEventRepo;
  }

  public Map<String, List<ListenerEvent>> findToBeSend() {
    return listenerEventRepo
        .findReadyToSend()
        .stream()
        .collect(Collectors.groupingBy(ListenerEvent::key));
  }

  public void doSendingMessage(String dbName) {
    while (true) {
      Map<String, List<ListenerEvent>> sendMap = findToBeSend();
      if (sendMap.isEmpty()) {
        try {
          logger.info("dbName:{},no data,thread sleeping", dbName);
          Thread.sleep(1000);
          continue;
        } catch (InterruptedException e) {
          logger.warn("error,thread is being interrupted!");
        }
      }
      sendInLoop(dbName, sendMap);
    }
  }

  private void sendInLoop(String dbName, Map<String, List<ListenerEvent>> sendMap) {
    LinkedList<Future<?>> results = new LinkedList<>();
    sendMap.forEach((k, v) -> {
      Future<?> result = taskExecutor.submit(() -> {
        v.forEach(
            x -> {
              DynamicDataSourceContextHolder.set(dbName);
              logger.debug("doTask-in-executer->" + dbName);
              sendAndUpdate(x);
              logger.debug("RocketMQ send data=>[{}]", x);
              DynamicDataSourceContextHolder.clearDataSourceKey();
            }
        );
      });
      results.add(result);
    });
    checkTask(results);
  }


  private void sendAndUpdate(ListenerEvent event) {
    mqProducer.send(event.getTopic(), event.getTag(), event.getContent());
    listenerEventRepo.updateStatus(event.getId(), SendingStatus.SENDED);
  }

  private void checkTask(LinkedList<Future<?>> results) {
    while (true) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        logger.error("error,thread is being interrupted!");
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
