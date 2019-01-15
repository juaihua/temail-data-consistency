package com.syswin.temail.data.consistency.application.impl;

import com.syswin.temail.data.consistency.application.MQProducer;
import com.syswin.temail.data.consistency.domain.SendingMQMessageException;
import java.io.UnsupportedEncodingException;
import java.util.UUID;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class RocketMQProducer implements MQProducer{

  private final DefaultMQProducer producer = new DefaultMQProducer("data-consistency");

  private String host;

  public RocketMQProducer(@Value("${app.consistency.rocketmq.host}") String host) {
    this.host = host;
  }

  @PostConstruct
  public void start() throws MQClientException {
    log.info("MQ producer start");
    producer.setNamesrvAddr(host);
    producer.setInstanceName(UUID.randomUUID().toString());
    producer.start();
  }

  @Override
  public void send(String body, String topic, String tags, String keys)
      throws UnsupportedEncodingException, InterruptedException, RemotingException, MQClientException, MQBrokerException {
    Message mqMsg = new Message(topic, tags, keys, body.getBytes(RemotingHelper.DEFAULT_CHARSET));
    log.info("MQ: send message: {}", body);
    SendResult sendResult = producer.send(mqMsg);
    log.info("MQ: send result: {}", sendResult);
    if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
      throw new SendingMQMessageException(sendResult.toString());
    }
  }

  @PreDestroy
  public void stop() {
    producer.shutdown();
    log.info("MQ producer shutdown");
  }
}
