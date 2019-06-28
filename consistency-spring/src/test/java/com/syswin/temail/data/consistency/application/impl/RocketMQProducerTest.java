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

import static com.seanyinx.github.unit.scaffolding.Randomness.uniquify;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus.SUCCESS;
import static org.apache.rocketmq.common.protocol.heartbeat.MessageModel.CLUSTERING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;

import com.syswin.temail.data.consistency.containers.RocketMqBrokerContainer;
import com.syswin.temail.data.consistency.containers.RocketMqNameServerContainer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.testcontainers.containers.Network;

public class RocketMQProducerTest {

  private static final int MQ_SERVER_PORT = 9876;
  private static final String NAMESRV = "namesrv";

  private static final Network NETWORK = Network.newNetwork();
  private static final RocketMqNameServerContainer rocketMqNameSrv = new RocketMqNameServerContainer()
      .withNetwork(NETWORK)
      .withNetworkAliases(NAMESRV)
      .withFixedExposedPort(MQ_SERVER_PORT, MQ_SERVER_PORT);

  private static final RocketMqBrokerContainer rocketMqBroker = new RocketMqBrokerContainer()
      .withNetwork(NETWORK)
      .withEnv("NAMESRV_ADDR", "namesrv:" + MQ_SERVER_PORT)
      .withFixedExposedPort(10909, 10909)
      .withFixedExposedPort(10911, 10911);

  private static final String TOPIC = "some-topic";
  private static final DefaultMQPushConsumer consumer = new DefaultMQPushConsumer();
  private static final List<String> messages = new ArrayList<>();
  private static final String MESSAGE = "hello";

  @ClassRule
  public static RuleChain rules = RuleChain.outerRule(rocketMqNameSrv).around(rocketMqBroker);

  private static final DefaultMQProducer mqProducer = new DefaultMQProducer(uniquify("test-producer-group"));

  @BeforeClass
  public static void setUp() throws Exception {
    createMqTopic();
    createMqConsumer();
  }

  private static void createMqTopic() throws MQClientException {
    mqProducer.setNamesrvAddr(rocketMqNameSrv.getContainerIpAddress() + ":" + MQ_SERVER_PORT);
    mqProducer.start();

    // ensure topic exists before consumer connects, or no message will be received
    waitAtMost(10, SECONDS).until(() -> {
      try {
        mqProducer.createTopic(mqProducer.getCreateTopicKey(), TOPIC, 1);
        return true;
      } catch (MQClientException e) {
        e.printStackTrace();
        return false;
      }
    });
  }

  private static void createMqConsumer() throws MQClientException {
    consumer.setConsumerGroup("consumer-group");
    consumer.setNamesrvAddr(rocketMqNameSrv.getContainerIpAddress() + ":" + MQ_SERVER_PORT);
    consumer.setMessageModel(CLUSTERING);
    consumer.setInstanceName(UUID.randomUUID().toString());
    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
    consumer.subscribe(TOPIC, "*");
    consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) -> {
      for (MessageExt msg : msgs) {
        messages.add(new String(msg.getBody()));
      }
      return SUCCESS;
    });

    consumer.start();
  }

  @AfterClass
  public static void tearDown() {
    mqProducer.shutdown();
    consumer.shutdown();
  }

  @Test
  public void shouldSendMessageToMQ() throws Exception {
    RocketMQProducer producer = new RocketMQProducer(rocketMqNameSrv.getContainerIpAddress() + ":" + MQ_SERVER_PORT);
    producer.start();

    producer.send(MESSAGE, TOPIC, "", "*");

    waitAtMost(5, SECONDS).untilAsserted(() -> assertThat(messages).containsExactly(MESSAGE));
    producer.stop();
  }
}
