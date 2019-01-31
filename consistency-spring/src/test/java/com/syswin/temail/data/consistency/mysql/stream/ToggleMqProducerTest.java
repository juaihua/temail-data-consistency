package com.syswin.temail.data.consistency.mysql.stream;

import static com.syswin.temail.data.consistency.mysql.stream.DataSyncFeature.BINLOG;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.syswin.temail.data.consistency.application.MQProducer;
import java.io.UnsupportedEncodingException;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;
import org.mockito.Mockito;
import org.togglz.core.manager.FeatureManager;

public class ToggleMqProducerTest {

  private final FeatureManager featureManager = Mockito.mock(FeatureManager.class);
  private final MQProducer mqProducer = Mockito.mock(MQProducer.class);
  private final ToggleMqProducer toggle = new ToggleMqProducer(featureManager, BINLOG, mqProducer);
  private final String body = "body";
  private final String topic = "topic";
  private final String tag = "tag";
  private final String key = "key";

  @Test
  public void runUnderlyingHandlerOnlyWhenEnabled()
      throws InterruptedException, RemotingException, MQClientException, MQBrokerException, UnsupportedEncodingException {
    when(featureManager.isActive(BINLOG)).thenReturn(true);

    toggle.send(body, topic, tag, key);
    verify(mqProducer).send(body, topic, tag, key);
  }

  @Test
  public void disableUnderlyingHandlerWhenOff()
      throws InterruptedException, RemotingException, MQClientException, MQBrokerException, UnsupportedEncodingException {
    when(featureManager.isActive(BINLOG)).thenReturn(false);

    toggle.send(body, topic, tag, key);
    verify(mqProducer, never()).send(body, topic, tag, key);
  }
}
