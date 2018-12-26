package com.syswin.temail.data.consistency.application;

import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

@Service
public class ListenerEventService {

  private static final Logger logger = LoggerFactory.getLogger(ListenerEventService.class);

  private final TaskService taskService;

  @Autowired
  public ListenerEventService(TaskService taskService) {
    this.taskService = taskService;
  }

  @Async("taskExecutor")
  public Future<String> doTask(String topic){
    logger.debug("doTask->"+topic);
    taskService.doSendingMessage(topic);

    return new AsyncResult<>("topic: " + topic + " ,task error");
  }
}
