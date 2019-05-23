package com.syswin.temail.data.consistency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;

import com.syswin.temail.data.consistency.application.MQProducer;
import com.syswin.temail.data.consistency.containers.MysqlContainer;
import com.syswin.temail.data.consistency.containers.ZookeeperContainer;
import java.util.ArrayList;
import java.util.List;
import org.awaitility.Duration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
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

  @ClassRule
  public static final RuleChain RULES = RuleChain.outerRule(mysql)
      .around(zookeeper);

  private final ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator();

  private final List<String> sentMessages = new ArrayList<>();

  @MockBean
  private MQProducer mqProducer;

  @BeforeClass
  public static void beforeClass() {
    String dbUrl = "jdbc:mysql://" + mysql.getContainerIpAddress() + ":" + mysql.getMappedPort(3306) + "/consistency?useSSL=false";
    System.setProperty("library.database.stream.multi.contexts[0].datasource.url", dbUrl);
    System.setProperty("library.database.stream.multi.contexts[0].datasource.username", "root");
    System.setProperty("library.database.stream.multi.contexts[0].datasource.password", "password");
    System.setProperty("library.database.stream.multi.contexts[0].cluster.name", "consistency");

    System.setProperty("spring.datasource.url", dbUrl);

    System.setProperty("library.database.stream.zk.address",
        zookeeper.getContainerIpAddress() + ":" + zookeeper.getMappedPort(2181));
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("library.database.stream.multi.contexts[0].url");
    System.clearProperty("library.database.stream.multi.contexts[0].datasource.username");
    System.clearProperty("library.database.stream.multi.contexts[0].datasource.password");
    System.clearProperty("library.database.stream.multi.contexts[0].cluster.name");

    System.clearProperty("spring.datasource.url");
    System.clearProperty("library.database.stream.zk.address");
  }

  @Before
  public void setUp() throws Exception {
    doAnswer(invocationOnMock -> {
      Object[] arguments = invocationOnMock.getArguments();
      return sentMessages.add(arguments[0] + "," + arguments[1] + "," + arguments[2]);
    }).when(mqProducer)
        .send(anyString(), anyString(), anyString(), anyString());

    databasePopulator.addScript(new ClassPathResource("data.sql"));
  }

  @Test
  public void streamEventsToMq() {

    waitAtMost(Duration.ONE_MINUTE).untilAsserted(() -> assertThat(sentMessages).hasSize(5));
    assertThat(sentMessages).containsExactly(
        "test1,bob,alice",
        "test2,jack,alice",
        "test3,bob,jack",
        "test4,john,bob",
        "test5,lucy,john"
    );
  }
}
