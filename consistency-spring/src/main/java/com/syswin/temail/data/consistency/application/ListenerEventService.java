package com.syswin.temail.data.consistency.application;

import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import com.syswin.temail.data.consistency.domain.SendingStatus;
import com.syswin.temail.data.consistency.interfaces.EventDataMonitorJob;
import com.syswin.temail.data.consistency.utils.JsonConverter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

@Service
public class ListenerEventService {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventDataMonitorJob.class);

  private final ThreadPoolTaskExecutor taskExecutor;

  private final MQProducer mqProducer;

  private final ListenerEventRepo listenerEventRepo;

  private final JsonConverter<ListenerEvent> jsonConverter;

  @Autowired
  public ListenerEventService(ThreadPoolTaskExecutor taskExecutor, MQProducer mqProducer,
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

  public void doSendingMessage() {
    Map<String, List<ListenerEvent>> sendMap = findToBeSend();
    sendMap.forEach((k, v) -> {
      taskExecutor.execute(() -> {
        v.forEach(
            x -> {
              mqProducer.send(x.getTopic(),x.getTag(),jsonConverter.toString(x));
              listenerEventRepo.updateStatus(x.getId(), SendingStatus.SENDED);
              LOGGER.debug("RocketMQ send data=>[{}]", x);
            }
        );
      });
    });
  }
}
