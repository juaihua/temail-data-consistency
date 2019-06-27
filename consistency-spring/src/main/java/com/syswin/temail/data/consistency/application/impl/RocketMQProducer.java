/*
 * MIT License
 *
 * Copyright (c) 2019 Syswin
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.syswin.temail.data.consistency.application.impl;

import com.syswin.temail.data.consistency.application.MQProducer;
import com.syswin.temail.data.consistency.domain.SendingMQMessageException;
import java.io.UnsupportedEncodingException;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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

  private final String host;
  // TODO: 2019/1/31 expose prometheus metric instead
  private final AtomicLong counter = new AtomicLong();
  private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
  private long previousCount = 0L;

  public RocketMQProducer(@Value("${app.consistency.rocketmq.host}") String host) {
    this.host = host;
  }

  @PostConstruct
  public void start() throws MQClientException {
    log.info("MQ producer start");
    producer.setNamesrvAddr(host);
    producer.setInstanceName(UUID.randomUUID().toString());
    producer.start();
    scheduledExecutor.scheduleWithFixedDelay(
        () -> {
          long count = counter.get();
          if (count > previousCount) {
            log.info("Sent {} messages since started", count);
            previousCount = count;
          }
        },
        10L, 10L, TimeUnit.SECONDS);
  }

  @Override
  public void send(String body, String topic, String tags, String keys)
      throws UnsupportedEncodingException, InterruptedException, RemotingException, MQClientException, MQBrokerException {
    Message mqMsg = new Message(topic, tags, keys, body.getBytes(RemotingHelper.DEFAULT_CHARSET));
    log.debug("MQ: sending message to topic: {} tag: {}", topic, tags);
    SendResult sendResult = loadBalancedSend(tags, mqMsg);
    log.debug("MQ: send result: {}", sendResult);
    if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
      throw new SendingMQMessageException(sendResult.toString());
    }
    counter.getAndIncrement();
  }

  private SendResult loadBalancedSend(String tag, Message message)
      throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

    return producer.send(message, (queues, msg, arg) -> {
      int index = Math.abs(arg.hashCode() % queues.size());
        return queues.get(index);
      }, tag);
  }

  @PreDestroy
  public void stop() {
    producer.shutdown();
    log.info("MQ producer shutdown");
    scheduledExecutor.shutdownNow();
  }
}
