package com.syswin.temail.data.consistency.application;

import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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

  @Transactional
  public void sendAndUpdate(ListenerEvent event) {
    mqProducer.send(event.getTopic(), event.getTag(), event.getContent());
    listenerEventRepo.delete(event.getId());
  }
}
