package com.syswin.temail.data.consistency.application.impl;

import com.syswin.temail.data.consistency.application.MQProducer;
import com.syswin.temail.data.consistency.domain.SendingMQMessageException;
import java.util.UUID;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

@Service
public class RocketMQProducer implements MQProducer{

  private static final Logger logger = LoggerFactory.getLogger(RocketMQProducer.class);
  private final DefaultMQProducer producer = new DefaultMQProducer("data-consistency");

  private String host;

  public RocketMQProducer(@Value("${app.consistency.rocketmq.host}") String host) {
    this.host = host;
  }

  @PostConstruct
  public void start() throws MQClientException {
    logger.info("MQ：启动生产者");
    producer.setNamesrvAddr(host);
    producer.setInstanceName(UUID.randomUUID().toString());
    producer.start();
  }

  @Override
  public boolean send(String topic, String tag, String content) {
    logger.debug("sendMessage-topic={}->{}", topic, content);
    Message mqMessage = new Message(topic, tag, (content).getBytes());
    StopWatch stop = new StopWatch();
    try {
      stop.start();
      SendResult result = producer.send(mqMessage, (mqs, msg, arg) -> {
        Integer id = (Integer) arg;
        int index = id % mqs.size();
        return mqs.get(index);
      }, 1);
      if (result.getSendStatus().equals(SendStatus.SEND_OK)) {
        return true;
      } else {
        logger.error("mq send message FAILURE,topic=[{}],message=[{}]", topic, content);
        return false;
      }
    } catch (Exception e) {
      logger.error("mq send message error,topic=[{}],message=[{}]", topic, content, e);
      throw new SendingMQMessageException(e);
    } finally {
      stop.stop();
    }
  }

  @PreDestroy
  public void stop() {
    if (producer != null) {
      producer.shutdown();
      logger.info("MQ：关闭生产者");
    }
  }
}
