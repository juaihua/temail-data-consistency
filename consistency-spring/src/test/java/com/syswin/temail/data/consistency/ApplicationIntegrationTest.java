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

package com.syswin.temail.data.consistency;

import static com.seanyinx.github.unit.scaffolding.Randomness.uniquify;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.rocketmq.common.protocol.heartbeat.MessageModel.CLUSTERING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;

import com.syswin.library.messaging.MessagingException;
import com.syswin.library.messaging.MqConsumer;
import com.syswin.library.messaging.rocketmq.ConcurrentRocketMqConsumer;
import com.syswin.temail.data.consistency.containers.MysqlContainer;
import com.syswin.temail.data.consistency.containers.RocketMqBrokerContainer;
import com.syswin.temail.data.consistency.containers.RocketMqNameServerContainer;
import com.syswin.temail.data.consistency.containers.ZookeeperContainer;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.awaitility.Duration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.Network;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestApplication.class}, properties = {
    "togglz.features.POLL.enabled=false",
    "togglz.features.BINLOG.enabled=true",
    "spring.autoconfigure.exclude[0]=com.systoon.integration.spring.boot.disconf.DisconfAutoConfiguration",
    "spring.autoconfigure.exclude[1]=com.systoon.integration.spring.boot.disconf.context.config.ConfigurationPropertiesRebinderAutoConfiguration",
    "spring.autoconfigure.exclude[2]=com.systoon.integration.spring.boot.disconf.context.config.RefreshAutoConfiguration",
    "library.database.stream.participant.id=1",
    "library.database.stream.multi.enabled=false",
    "library.database.stream.election.enabled=true",
    "library.database.stream.cluster.name=dev",
    "library.messaging.rocketmq.enabled=true",
    "app.consistency.binlog.housekeeper.sweep.interval=60000",
    "spring.datasource.username=root",
    "spring.datasource.password=password"
})
@ActiveProfiles("dev")
public class ApplicationIntegrationTest {

  private static final Network NETWORK = Network.newNetwork();

  private static final MysqlContainer mysql = new MysqlContainer().withNetwork(NETWORK)
      .withNetworkAliases("mysql-temail")
      .withEnv("MYSQL_DATABASE", "consistency")
      .withEnv("MYSQL_USER", "syswin")
      .withEnv("MYSQL_PASSWORD", "password")
      .withEnv("MYSQL_ROOT_PASSWORD", "password");

  private static final ZookeeperContainer zookeeper = new ZookeeperContainer()
      .withNetwork(NETWORK)
      .withNetworkAliases("zookeeper-temail");

  private static final int MQ_SERVER_PORT = 9876;
  private static final String NAMESRV = "namesrv";

  private static final RocketMqNameServerContainer rocketMqNameSrv = new RocketMqNameServerContainer()
      .withNetwork(NETWORK)
      .withNetworkAliases(NAMESRV)
      .withFixedExposedPort(MQ_SERVER_PORT, MQ_SERVER_PORT);

  private static final RocketMqBrokerContainer rocketMqBroker = new RocketMqBrokerContainer()
      .withNetwork(NETWORK)
      .withEnv("NAMESRV_ADDR", "namesrv:" + MQ_SERVER_PORT)
      .withFixedExposedPort(10909, 10909)
      .withFixedExposedPort(10911, 10911);

  private static final DefaultMQProducer mqProducer = new DefaultMQProducer(uniquify("test-producer-group"));

  @ClassRule
  public static final RuleChain RULES = RuleChain.outerRule(mysql)
      .around(zookeeper)
      .around(rocketMqNameSrv)
      .around(rocketMqBroker);

  private final ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator();

  private static final List<String> sentMessages = new ArrayList<>();
  private static final String[] topics = new String[]{"bob", "jack", "bob", "john", "lucy"};
  private static final String[] tags = new String[]{"alice", "alice", "jack", "bob", "john"};

  private static String brokerAddress;
  private static final List<MqConsumer> mqConsumers = new ArrayList<>(topics.length);

  @BeforeClass
  public static void beforeClass() throws MQClientException, MessagingException {
    brokerAddress = rocketMqNameSrv.getContainerIpAddress() + ":" + MQ_SERVER_PORT;
    createMqTopic();
    createMqConsumer();

    String dbUrl = "jdbc:mysql://" + mysql.getContainerIpAddress() + ":" + mysql.getMappedPort(3306) + "/consistency?useSSL=false";
    System.setProperty("library.database.stream.multi.contexts[0].datasource.url", dbUrl);
    System.setProperty("library.database.stream.multi.contexts[0].datasource.username", "root");
    System.setProperty("library.database.stream.multi.contexts[0].datasource.password", "password");
    System.setProperty("library.database.stream.multi.contexts[0].cluster.name", "consistency");

    System.setProperty("spring.datasource.url", dbUrl);
    System.setProperty("spring.rocketmq.host", brokerAddress);

    System.setProperty("library.database.stream.zk.address",
        zookeeper.getContainerIpAddress() + ":" + zookeeper.getMappedPort(2181));
  }

  @AfterClass
  public static void afterClass() {
    mqProducer.shutdown();
    for (MqConsumer mqConsumer : mqConsumers) {
      mqConsumer.shutdown();
    }

    System.clearProperty("library.database.stream.multi.contexts[0].url");
    System.clearProperty("library.database.stream.multi.contexts[0].datasource.username");
    System.clearProperty("library.database.stream.multi.contexts[0].datasource.password");
    System.clearProperty("library.database.stream.multi.contexts[0].cluster.name");

    System.clearProperty("spring.datasource.url");
    System.clearProperty("spring.rocketmq.host");
    System.clearProperty("library.database.stream.zk.address");
  }

  private static void createMqTopic() throws MQClientException {
    mqProducer.setNamesrvAddr(brokerAddress);
    mqProducer.start();

    // ensure topic exists before consumer connects, or no message will be received
    waitAtMost(10, SECONDS).until(() -> {
      try {
        for (String topic : topics) {
          mqProducer.createTopic(mqProducer.getCreateTopicKey(), topic, 4);
        }
        return true;
      } catch (MQClientException e) {
        e.printStackTrace();
        return false;
      }
    });
  }

  private static void createMqConsumer() throws MessagingException {
    for (int i = 0; i < topics.length; i++) {
      String topic = topics[i];
      String tag = tags[i];
      ConcurrentRocketMqConsumer consumer = new ConcurrentRocketMqConsumer(
          brokerAddress,
          uniquify("consumer-concurrent"),
          topic,
          tag,
          CLUSTERING,
          msg -> sentMessages.add(msg + "," + topic + "," + tag));

      consumer.start();
      mqConsumers.add(consumer);
    }
  }

  @Before
  public void setUp() {
    databasePopulator.addScript(new ClassPathResource("data.sql"));
  }

  @Test
  public void streamEventsToMq() {

    waitAtMost(Duration.TWO_MINUTES).untilAsserted(() -> assertThat(sentMessages).hasSize(5));
    assertThat(sentMessages).containsExactly(
        "test1,bob,alice",
        "test2,jack,alice",
        "test3,bob,jack",
        "test4,john,bob",
        "test5,lucy,john"
    );
  }
}
