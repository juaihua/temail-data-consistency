package com.syswin.temail.data.consistency.application;


import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.infrastructure.ListenerEventMapper;
import org.springframework.beans.factory.annotation.Autowired;

public class ListenerEventService {

  @Autowired
  private ListenerEventMapper eventMapper;

  public int saveEvent(ListenerEvent listenerEvent) {
    return eventMapper.insert(listenerEvent);
  }

}
