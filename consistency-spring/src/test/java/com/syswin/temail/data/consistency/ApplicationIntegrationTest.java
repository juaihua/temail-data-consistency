package com.syswin.temail.data.consistency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;

import com.syswin.temail.data.consistency.application.MQProducer;
import com.syswin.temail.data.consistency.containers.MysqlContainer;
import com.syswin.temail.data.consistency.mysql.stream.BinlogSyncRecorder;
import com.syswin.temail.data.consistency.mysql.stream.MqEventSender;
import com.syswin.temail.data.consistency.mysql.stream.MysqlBinLogStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import javax.sql.DataSource;
import org.awaitility.Duration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.Network;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestApplication.class)
@ActiveProfiles("dev")
public class ApplicationIntegrationTest {

  private static final Network NETWORK = Network.newNetwork();

  @ClassRule
  public static final MysqlContainer mysql = new MysqlContainer().withNetwork(NETWORK)
      .withNetworkAliases("mysql-temail")
      .withEnv("MYSQL_DATABASE", "consistency")
      .withEnv("MYSQL_USER", "syswin")
      .withEnv("MYSQL_PASSWORD", "password")
      .withEnv("MYSQL_ROOT_PASSWORD", "password");

  private final ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator();

  private final List<String> sentMessages = new ArrayList<>();
  private final MQProducer mqProducer = (body, topic, tags, keys) -> sentMessages.add(body + "," + topic + "," + tags);
  private final MqEventSender eventHandler = new MqEventSender(mqProducer, 1000L);

  private final MockBinlogSyncRecorder binlogSyncRecorder = new MockBinlogSyncRecorder();

  @Autowired
  private DataSource dataSource;
  private MysqlBinLogStream mysqlBinLogStream;

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("spring.datasource.url",
        "jdbc:mysql://" + mysql.getContainerIpAddress() + ":" + mysql.getMappedPort(3306) + "/consistency?useSSL=false");
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("spring.datasource.url");
  }

  @Before
  public void setUp() {
    databasePopulator.addScript(new ClassPathResource("data.sql"));
    mysqlBinLogStream = new MysqlBinLogStream(
        mysql.getContainerIpAddress(),
        mysql.getMappedPort(3306),
        "root",
        "password",
        binlogSyncRecorder
    );
  }

  @After
  public void tearDown() {
    mysqlBinLogStream.stop();
  }

  @Test
  public void streamEventsToMq() throws IOException, TimeoutException, InterruptedException {
    mysqlBinLogStream.start(eventHandler, "listener_event");

    DatabasePopulatorUtils.execute(databasePopulator, dataSource);

    waitAtMost(Duration.ONE_SECOND).until(() -> sentMessages.size() == 5);
    assertThat(sentMessages).containsExactly(
        "test1,bob,alice",
        "test2,jack,alice",
        "test3,bob,jack",
        "test4,john,bob",
        "test5,lucy,john"
    );

    mysqlBinLogStream.stop();
    DatabasePopulatorUtils.execute(databasePopulator, dataSource);
    mysqlBinLogStream.start(eventHandler, "listener_event");

    // resume for last known position
    waitAtMost(Duration.ONE_SECOND).until(() -> sentMessages.size() == 10);
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

    mysqlBinLogStream.stop();
    DatabasePopulatorUtils.execute(databasePopulator, dataSource);
    binlogSyncRecorder.reset();
    mysqlBinLogStream.start(eventHandler, "listener_event");
    Thread.sleep(1000);

    // start from latest binlog, so no new event processed
    assertThat(sentMessages).hasSize(10);
  }

  private static class MockBinlogSyncRecorder implements BinlogSyncRecorder {

    private String binlogFilename;
    private long binLogPosition;

    @Override
    public void record(String filename, long position) {
      binlogFilename = filename;
      binLogPosition = position;
    }

    @Override
    public String filename() {
      return binlogFilename;
    }

    @Override
    public long position() {
      return binLogPosition;
    }

    void reset() {
      binlogFilename = null;
      binLogPosition = 0;
    }
  }
}
