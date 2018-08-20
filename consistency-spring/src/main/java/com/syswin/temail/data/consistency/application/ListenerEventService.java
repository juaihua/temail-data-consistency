package com.syswin.temail.data.consistency.application;

import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ListenerEventService {

  private final ListenerEventRepo listenerEventRepo;

  @Autowired
  public ListenerEventService(ListenerEventRepo listenerEventRepo) {
    this.listenerEventRepo = listenerEventRepo;
  }

  public Map<String, List<ListenerEvent>> findToBeSend(){
    List<ListenerEvent> listenerEvents = listenerEventRepo.findReadyToSend();
    return listenerEvents.stream().collect(Collectors.groupingBy(ListenerEvent::getToAddr));
  }
}
