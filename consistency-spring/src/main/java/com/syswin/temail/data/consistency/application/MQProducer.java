package com.syswin.temail.data.consistency.application;

public interface MQProducer {

  boolean send(String topic, String tag, String content);
}
