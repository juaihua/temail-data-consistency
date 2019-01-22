package com.syswin.temail.data.consistency.mysql.stream;

import static com.syswin.temail.data.consistency.domain.SendingStatus.NEW;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.syswin.temail.data.consistency.application.MQProducer;
import com.syswin.temail.data.consistency.domain.ListenerEvent;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;
import org.mockito.Mockito;

public class MqEventSenderTest {

  private final List<String> sentMessages = new ArrayList<>();
  private final MQProducer mqProducer = Mockito.mock(MQProducer.class);
  private final MQProducer compositeMqProducer = (body, topic, tags, keys) -> {
    mqProducer.send(body, topic, tags, keys);
    sentMessages.add(body + "," + topic + "," + tags);
  };

  private final MqEventSender sender = new MqEventSender(compositeMqProducer, 100L);
  private final ListenerEvent listenerEvent1 = new ListenerEvent(NEW, "foo", "private", "aaa");
  private final ListenerEvent listenerEvent2 = new ListenerEvent(NEW, "bar", "private", "bbb");

  @Test
  public void sendEvent() throws Exception {
    sender.handle(asList(
        listenerEvent1,
        listenerEvent2));

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
    doThrow(RemotingException.class, MQClientException.class, MQBrokerException.class)
        .doNothing()
        .when(mqProducer)
        .send(anyString(), anyString(), anyString(), anyString());

    sender.handle(asList(
        new ListenerEvent(NEW, "foo", "private", "aaa"),
        new ListenerEvent(NEW, "bar", "private", "bbb")));

    assertThat(sentMessages).containsExactly(
        "foo,private,aaa",
        "bar,private,bbb"
    );
  }

  @Test
  public void stopRetryingWhenInterrupted() throws Exception {
    doThrow(RemotingException.class)
        .when(mqProducer)
        .send(anyString(), anyString(), anyString(), anyString());

    AtomicBoolean interrupted = new AtomicBoolean(false);
    Thread thread = new Thread(() -> {
      sender.handle(asList(
          new ListenerEvent(NEW, "foo", "private", "aaa"),
          new ListenerEvent(NEW, "bar", "private", "bbb")));
      interrupted.set(true);
    });

    thread.start();
    Thread.sleep(300L);
    thread.interrupt();

    assertThat(sentMessages).isEmpty();
    await().atMost(1, SECONDS).untilTrue(interrupted);
  }

  @Test(timeout = 2000L)
  public void skipSendingEventsWhenUnsupportedEncoding() throws Exception {
    doThrow(UnsupportedEncodingException.class)
        .when(mqProducer)
        .send(anyString(), anyString(), anyString(), anyString());

    sender.handle(asList(
        new ListenerEvent(NEW, "foo", "private", "aaa"),
        new ListenerEvent(NEW, "bar", "private", "bbb")));

    assertThat(sentMessages).isEmpty();
  }

  @Test(timeout = 2000L)
  public void skipSendingEventsWhenInterrupted() throws Exception {
    doThrow(InterruptedException.class)
        .when(mqProducer)
        .send(anyString(), anyString(), anyString(), anyString());

    sender.handle(asList(
        new ListenerEvent(NEW, "foo", "private", "aaa"),
        new ListenerEvent(NEW, "bar", "private", "bbb")));

    assertThat(sentMessages).isEmpty();

    verify(mqProducer).send(listenerEvent1.getContent(), listenerEvent1.getTopic(), listenerEvent1.getTag(), listenerEvent1.key());
    verify(mqProducer, never()).send(listenerEvent2.getContent(), listenerEvent2.getTopic(), listenerEvent2.getTag(), listenerEvent2.key());
  }
}