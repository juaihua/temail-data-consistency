package com.syswin.temail.data.consistency.application;

public interface MQProducer {

  void send(String content);
}
