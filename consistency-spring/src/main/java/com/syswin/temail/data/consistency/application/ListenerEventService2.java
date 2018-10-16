package com.syswin.temail.data.consistency.application;

import com.syswin.temail.data.consistency.configuration.datasource.DynamicDataSourceContextHolder;
import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import com.syswin.temail.data.consistency.domain.SendingStatus;
import com.syswin.temail.data.consistency.interfaces.EventDataMonitorJob;
import com.syswin.temail.data.consistency.utils.JsonConverter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class ListenerEventService2 {

  private static final Logger logger = LoggerFactory.getLogger(EventDataMonitorJob.class);

  private final ThreadPoolTaskExecutor taskExecutor;

  private final MQProducer mqProducer;

  private final ListenerEventRepo listenerEventRepo;

  private final JsonConverter<ListenerEvent> jsonConverter;

  @Autowired
  public ListenerEventService2(ThreadPoolTaskExecutor taskExecutor, MQProducer mqProducer,
      ListenerEventRepo listenerEventRepo,
      JsonConverter<ListenerEvent> jsonConverter) {
    this.taskExecutor = taskExecutor;
    this.mqProducer = mqProducer;
    this.listenerEventRepo = listenerEventRepo;
    this.jsonConverter = jsonConverter;
  }

  public Map<String, List<ListenerEvent>> findToBeSend(){
    return listenerEventRepo
        .findReadyToSend()
        .stream()
        .collect(Collectors.groupingBy(ListenerEvent::key));
  }

  @Async("taskExecutor")
  public Future<String> doTask(String dbName){
    DynamicDataSourceContextHolder.set(dbName);
    logger.debug("doTask->"+dbName);
    doSendingMessage();
    DynamicDataSourceContextHolder.clearDataSourceKey();
    return new AsyncResult<>("database: " + dbName + " ,task error");
  }

  public void doSendingMessage(){
    while (true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        logger.error("error,thread is being interrupted!");
      }
      Map<String, List<ListenerEvent>> sendMap = findToBeSend();
      sendMap.forEach((k, v) -> {
        taskExecutor.execute(() -> {
          v.forEach(
              x -> {
                mqProducer.send(x.getTopic(),x.getTag(),x.getContent());
                listenerEventRepo.updateStatus(x.getId(), SendingStatus.SENDED);
                logger.debug("RocketMQ send data=>[{}]", x);
              }
          );
        });
      });
    }
  }
}
