package com.syswin.temail.data.consistency.application;


import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.SendingStatus;
import com.syswin.temail.data.consistency.infrastructure.ListenerEventMapper;
import org.springframework.beans.factory.annotation.Autowired;

public class TemailMqSender {

  @Autowired
  private ListenerEventMapper eventMapper;

  public int saveEvent(String topic, String tag, String message) {
    ListenerEvent listenerEvent = new ListenerEvent(SendingStatus.NEW, message, topic, tag);
    return eventMapper.insert(listenerEvent);
  }

}
