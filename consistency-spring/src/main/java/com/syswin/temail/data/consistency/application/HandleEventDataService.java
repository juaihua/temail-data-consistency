package com.syswin.temail.data.consistency.application;

import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import com.syswin.temail.data.consistency.domain.SendingMQMessageException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class HandleEventDataService {

  private final MQProducer mqProducer;

  private final ListenerEventRepo listenerEventRepo;


  @Autowired
  public HandleEventDataService(MQProducer mqProducer, ListenerEventRepo listenerEventRepo) {
    this.mqProducer = mqProducer;
    this.listenerEventRepo = listenerEventRepo;
  }

  public Map<String, List<ListenerEvent>> findToBeSend(String topic) {
    return listenerEventRepo
        .findReadyToSend(topic)
        .stream()
        .collect(Collectors.groupingBy(ListenerEvent::key));
  }

  public void sendAndUpdate(ListenerEvent event) {
    try {
      mqProducer.send(event.getContent(),event.getTopic(),event.getTag(),"");
    } catch (Exception e) {
      log.error("send message to MQ error!");
      throw new SendingMQMessageException(e);
    }
    listenerEventRepo.delete(event.getId());
  }
}
