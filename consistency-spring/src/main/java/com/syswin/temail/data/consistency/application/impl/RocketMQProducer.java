package com.syswin.temail.data.consistency.application.impl;

import com.syswin.temail.data.consistency.application.MQProducer;
import org.apache.rocketmq.spring.starter.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
public class RocketMQProducer implements MQProducer{

  private final RocketMQTemplate rocketMQTemplate;

  private static final String DESTINATION = "consistency-destination";

  @Autowired
  public RocketMQProducer(RocketMQTemplate rocketMQTemplate) {
    this.rocketMQTemplate = rocketMQTemplate;
  }

  @Override
  public void send(String content) {
    rocketMQTemplate.send(DESTINATION, MessageBuilder.withPayload(content).build());
  }
}
