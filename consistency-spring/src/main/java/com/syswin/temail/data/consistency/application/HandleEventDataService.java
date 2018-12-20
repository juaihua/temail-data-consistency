package com.syswin.temail.data.consistency.application;

import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import com.syswin.temail.data.consistency.domain.SendingStatus;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class HandleEventDataService {

  private final MQProducer mqProducer;

  private final ListenerEventRepo listenerEventRepo;

  private final String timeCondition;

  @Autowired
  public HandleEventDataService( MQProducer mqProducer, ListenerEventRepo listenerEventRepo,
  @Value("${app.consistency.flush.data.time.condition:7}") String timeCondition) {
    this.mqProducer = mqProducer;
    this.listenerEventRepo = listenerEventRepo;
    this.timeCondition = timeCondition;
  }

  public Map<String, List<ListenerEvent>> findToBeSend(String topic) {
    return listenerEventRepo
        .findReadyToSend(topic)
        .stream()
        .collect(Collectors.groupingBy(ListenerEvent::key));
  }

  @Transactional
  public void sendAndUpdate(ListenerEvent event) {
    listenerEventRepo.updateStatus(event.getId(), SendingStatus.SENT);
    mqProducer.send(event.getTopic(), event.getTag(), event.getContent());
  }

  public void flush() {
    LocalDateTime condition = LocalDateTime.now().minusDays(Long.parseLong(timeCondition));
    listenerEventRepo.deleteByCondition(condition);
  }

  public static void main(String[] args) {
    LocalDateTime now = LocalDateTime.now();
    System.out.println(now);
  }
}
