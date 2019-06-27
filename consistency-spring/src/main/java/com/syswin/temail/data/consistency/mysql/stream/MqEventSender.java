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

package com.syswin.temail.data.consistency.mysql.stream;

import com.syswin.temail.data.consistency.application.MQProducer;
import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.SendingMQMessageException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.exception.RemotingException;

@Slf4j
public class MqEventSender implements EventHandler {

  private final MQProducer mqProducer;
  private final int maxRetries;
  private final long retryIntervalMillis;

  MqEventSender(MQProducer mqProducer, int maxRetries, long retryIntervalMillis) {
    this.mqProducer = mqProducer;
    this.maxRetries = maxRetries;
    this.retryIntervalMillis = retryIntervalMillis;
  }

  @Override
  public void handle(List<ListenerEvent> listenerEvents) {
    for (int i = 0; i < listenerEvents.size() && !Thread.currentThread().isInterrupted(); i++) {
      send(listenerEvents.get(i));
    }
  }

  private void send(ListenerEvent listenerEvent) {
    for (int i = 0; !Thread.currentThread().isInterrupted(); i++) {
      try {
        log.debug("Sending event to MQ: {}", listenerEvent);
        mqProducer.send(listenerEvent.getContent(), listenerEvent.getTopic(), listenerEvent.getTag(), listenerEvent.key());
        return;
      } catch (RemotingException | MQBrokerException e) {
        log.error("Failed to send listener event by MQ and will retry: {}", listenerEvent, e);
        if (i == maxRetries) {
          throw new SendingMQMessageException("Failed to send listener event by MQ after retrying " + maxRetries + " times", e);
        }
        sleep(listenerEvent);
      } catch (SendingMQMessageException e) {
        log.error("Failed to send listener event by MQ and will retry: {}", listenerEvent, e);
        sleep(listenerEvent);
      } catch (InterruptedException e) {
        onInterruption(listenerEvent, e);
      } catch (UnsupportedEncodingException | MQClientException e) {
        log.error("Failed to send listener event by MQ: {}", listenerEvent, e);
        return;
      }
    }
  }

  private void sleep(ListenerEvent listenerEvent) {
    try {
      Thread.sleep(retryIntervalMillis);
    } catch (InterruptedException e) {
      onInterruption(listenerEvent, e);
    }
  }

  private void onInterruption(ListenerEvent listenerEvent, InterruptedException e) {
    log.warn("Failed to send listener event by MQ due to interruption: {}", listenerEvent, e);
    Thread.currentThread().interrupt();
    throw new SendingMQMessageException("Failed to send listener event by MQ due to interruption", e);
  }
}
