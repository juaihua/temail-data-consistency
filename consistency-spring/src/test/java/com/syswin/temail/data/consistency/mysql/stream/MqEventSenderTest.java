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

import static com.seanyinx.github.unit.scaffolding.AssertUtils.expectFailing;
import static com.syswin.temail.data.consistency.domain.SendingStatus.NEW;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.syswin.library.messaging.MessageBrokerException;
import com.syswin.library.messaging.MessageClientException;
import com.syswin.library.messaging.MessageDeliverException;
import com.syswin.library.messaging.MessagingException;
import com.syswin.library.messaging.MqProducer;
import com.syswin.temail.data.consistency.application.MQProducer;
import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.SendingMQMessageException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.mockito.Mockito;

public class MqEventSenderTest {

  private final List<String> sentMessages = new ArrayList<>();
  private final MqProducer mqProducer = Mockito.mock(MqProducer.class);
  private final AtomicBoolean started = new AtomicBoolean();
  private final MQProducer compositeMqProducer = new MQProducer() {
    @Override
    public void start() {
      started.set(true);
    }

    @Override
    public void stop() {
      started.set(false);
    }

    @Override
    public void send(String body, String topic, String tags, String keys)
        throws UnsupportedEncodingException, InterruptedException, MessagingException {
      mqProducer.send(body, topic, tags, keys);
      sentMessages.add(body + "," + topic + "," + tags);
    }
  };

  private final int maxRetries = 3;
  private final MqEventSender sender = new MqEventSender(compositeMqProducer, maxRetries, 100L);
  private final ListenerEvent listenerEvent1 = new ListenerEvent(NEW, "foo", "private", "aaa");
  private final ListenerEvent listenerEvent2 = new ListenerEvent(NEW, "bar", "private", "bbb");
  private final List<ListenerEvent> listenerEvents = asList(listenerEvent1, listenerEvent2);

  @Test
  public void startUnderlyingMqProducer() {
    sender.start();
    assertThat(started).isTrue();

    sender.stop();
    assertThat(started).isFalse();
  }

  @Test
  public void sendEvent() throws Exception {
    sender.handle(listenerEvents);

    assertThat(sentMessages).containsExactly(
        "foo,private,aaa",
        "bar,private,bbb"
    );

    verify(mqProducer).send(listenerEvent1.getContent(), listenerEvent1.getTopic(), listenerEvent1.getTag(), listenerEvent1.key());
    verify(mqProducer).send(listenerEvent2.getContent(), listenerEvent2.getTopic(), listenerEvent2.getTag(), listenerEvent2.key());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void retryFailedEventsWhenMqOutOfOrder() throws Exception {
    doThrow(MessageDeliverException.class)
        .doNothing()
        .when(mqProducer)
        .send(anyString(), anyString(), anyString(), anyString());

    sender.handle(listenerEvents);

    assertThat(sentMessages).containsExactly(
        "foo,private,aaa",
        "bar,private,bbb"
    );
  }

  @Test
  public void stopRetryingWhenInterrupted() throws Exception {
    doThrow(MessageBrokerException.class)
        .when(mqProducer)
        .send(anyString(), anyString(), anyString(), anyString());

    AtomicBoolean interrupted = new AtomicBoolean(false);
    Thread thread = new Thread(() -> {
      try {
        sender.handle(listenerEvents);
      } catch (SendingMQMessageException e) {
        interrupted.set(true);
      }
    });

    thread.start();
    Thread.sleep(500L);
    thread.interrupt();

    assertThat(sentMessages).isEmpty();
    await().atMost(1, SECONDS).untilTrue(interrupted);
    verify(mqProducer, atLeast(maxRetries + 1)).send(anyString(), anyString(), anyString(), anyString());
  }

  @Test(timeout = 2000L)
  public void retryUpToSpecifiedTimes() throws Exception {
    doThrow(MessageDeliverException.class)
        .when(mqProducer)
        .send(anyString(), anyString(), anyString(), anyString());

    try {
      sender.handle(listenerEvents);
      expectFailing(SendingMQMessageException.class);
    } catch (SendingMQMessageException e) {
      e.printStackTrace();
    }

    assertThat(sentMessages).isEmpty();
    verify(mqProducer, times(maxRetries + 1)).send(anyString(), anyString(), anyString(), anyString());
  }

  @Test(timeout = 2000L)
  public void skipSendingInvalidEvents() throws Exception {
    doThrow(UnsupportedEncodingException.class, MessageClientException.class)
        .when(mqProducer)
        .send(anyString(), anyString(), anyString(), anyString());

    sender.handle(listenerEvents);
    sender.handle(listenerEvents);

    assertThat(sentMessages).isEmpty();
  }

  @Test(timeout = 2000L)
  public void skipSendingEventsWhenInterrupted() throws Exception {
    doThrow(InterruptedException.class)
        .when(mqProducer)
        .send(anyString(), anyString(), anyString(), anyString());

    try {
      sender.handle(listenerEvents);
      expectFailing(SendingMQMessageException.class);
    } catch (SendingMQMessageException ignored) {
    }

    assertThat(sentMessages).isEmpty();

    verify(mqProducer).send(listenerEvent1.getContent(), listenerEvent1.getTopic(), listenerEvent1.getTag(), listenerEvent1.key());
    verify(mqProducer, never()).send(listenerEvent2.getContent(), listenerEvent2.getTopic(), listenerEvent2.getTag(), listenerEvent2.key());
  }
}