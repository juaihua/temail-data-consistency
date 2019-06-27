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
