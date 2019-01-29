package com.syswin.temail.data.consistency;

import static com.syswin.temail.data.consistency.mysql.stream.ZkBinlogSyncRecorder.BINLOG_POSITION_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;

import com.syswin.temail.data.consistency.StatefulTaskConfig.StoppableStatefulTask;
import com.syswin.temail.data.consistency.application.MQProducer;
import com.syswin.temail.data.consistency.containers.MysqlContainer;
import com.syswin.temail.data.consistency.containers.ZookeeperContainer;
import com.syswin.temail.data.consistency.mysql.stream.StatefulTask;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.sql.DataSource;
import org.apache.curator.framework.CuratorFramework;
import org.awaitility.Duration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.Network;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {TestApplication.class, StatefulTaskConfig.class})
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

  private final ExecutorService executor = Executors.newSingleThreadExecutor();

  @Autowired
  private DataSource dataSource;

  @Autowired
  private StatefulTask mysqlBinLogStream;

  @Autowired
  private CuratorFramework curator;

  @Autowired
  private StoppableStatefulTask statefulTask;

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("spring.datasource.url",
        "jdbc:mysql://" + mysql.getContainerIpAddress() + ":" + mysql.getMappedPort(3306) + "/consistency?useSSL=false");

    System.setProperty("app.consistency.binlog.zk.address",
        zookeeper.getContainerIpAddress() + ":" + zookeeper.getMappedPort(2181));
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("spring.datasource.url");
    System.clearProperty("app.consistency.binlog.zk.address");
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

  @After
  public void tearDown() {
    executor.shutdownNow();
  }

  @Test
  public void streamEventsToMq() throws Exception {

    DatabasePopulatorUtils.execute(databasePopulator, dataSource);

    waitAtMost(Duration.ONE_SECOND).until(() -> sentMessages.size() == 5);
    assertThat(sentMessages).containsExactly(
        "test1,bob,alice",
        "test2,jack,alice",
        "test3,bob,jack",
        "test4,john,bob",
        "test5,lucy,john"
    );

    // simulate network interruption
    mysqlBinLogStream.stop();
    DatabasePopulatorUtils.execute(databasePopulator, dataSource);

    // resume for last known position
    waitAtMost(Duration.ONE_SECOND).untilAsserted(() -> assertThat(sentMessages).hasSize(10));
    assertThat(sentMessages).containsExactly(
        "test1,bob,alice",
        "test2,jack,alice",
        "test3,bob,jack",
        "test4,john,bob",
        "test5,lucy,john",
        "test1,bob,alice",
        "test2,jack,alice",
        "test3,bob,jack",
        "test4,john,bob",
        "test5,lucy,john"
    );

    statefulTask.pause();
    mysqlBinLogStream.stop();
    DatabasePopulatorUtils.execute(databasePopulator, dataSource);

    // reset binlog position
    curator.delete().forPath(BINLOG_POSITION_PATH);
    statefulTask.resume();
    Thread.sleep(1000);

    // start from latest binlog, so no new event processed
    assertThat(sentMessages).hasSize(10);
  }
}
