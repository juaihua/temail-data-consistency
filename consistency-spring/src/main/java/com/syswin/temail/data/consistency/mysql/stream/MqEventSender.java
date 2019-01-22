package com.syswin.temail.data.consistency.mysql.stream;

import com.syswin.temail.data.consistency.application.MQProducer;
import com.syswin.temail.data.consistency.domain.ListenerEvent;
import java.io.UnsupportedEncodingException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.exception.RemotingException;

@Slf4j
public class MqEventSender implements EventHandler {

  private final MQProducer mqProducer;
  private final long retryIntervalMillis;

  public MqEventSender(MQProducer mqProducer, long retryIntervalMillis) {
    this.mqProducer = mqProducer;
    this.retryIntervalMillis = retryIntervalMillis;
  }

  // TODO: 2019/1/22 batch sending?
  @Override
  public void handle(List<ListenerEvent> listenerEvents) {
    for (int i = 0; i < listenerEvents.size() && !Thread.currentThread().isInterrupted(); i++) {
      send(listenerEvents.get(i));
    }
  }

  private void send(ListenerEvent listenerEvent) {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        mqProducer.send(listenerEvent.getContent(), listenerEvent.getTopic(), listenerEvent.getTag(), listenerEvent.key());
        return;
      } catch (RemotingException | MQClientException | MQBrokerException e) {
        log.error("Failed to send listener event by MQ and will retry: {}", listenerEvent, e);
        sleep();
      } catch (InterruptedException e) {
        log.warn("Failed to send listener event by MQ due to interruption: {}", listenerEvent, e);
        Thread.currentThread().interrupt();
      } catch (UnsupportedEncodingException e) {
        log.error("Failed to send listener event by MQ due to unsupported encoding: {}", listenerEvent, e);
        return;
      }
    }
  }

  private void sleep() {
    try {
      Thread.sleep(retryIntervalMillis);
    } catch (InterruptedException e) {
      log.warn("Failed to retry sending listener event due to interruption", e);
      Thread.currentThread().interrupt();
    }
  }
}
