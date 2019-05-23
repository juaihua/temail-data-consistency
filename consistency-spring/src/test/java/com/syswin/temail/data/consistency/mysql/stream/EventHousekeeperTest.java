package com.syswin.temail.data.consistency.mysql.stream;

import static org.assertj.core.api.Assertions.assertThat;

import com.syswin.temail.data.consistency.application.MQProducer;
import com.syswin.temail.data.consistency.containers.MysqlContainer;
import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import com.syswin.temail.data.consistency.infrastructure.DbTestApplication;
import java.util.List;
import javax.sql.DataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = DbTestApplication.class, properties = {
    "spring.autoconfigure.exclude[0]=com.systoon.integration.spring.boot.disconf.DisconfAutoConfiguration",
    "spring.autoconfigure.exclude[1]=com.systoon.integration.spring.boot.disconf.context.config.ConfigurationPropertiesRebinderAutoConfiguration",
    "spring.autoconfigure.exclude[2]=com.systoon.integration.spring.boot.disconf.context.config.RefreshAutoConfiguration",
    "spring.autoconfigure.exclude[3]=com.syswin.library.database.event.stream.mysql.DataSourceContainer",
    "spring.autoconfigure.exclude[4]=com.syswin.library.database.event.stream.mysql.StatefulTaskComposerConfig",
    "spring.autoconfigure.exclude[5]=com.syswin.library.database.event.stream.mysql.DefaultMultiDataSourceConfig",
    "spring.autoconfigure.exclude[6]=com.syswin.library.database.event.stream.zookeeper.DefaultCuratorConfig",
    "spring.autoconfigure.exclude[7]=com.syswin.library.database.event.stream.mysql.DefaultBinlogStreamConfig",
    "spring.autoconfigure.exclude[8]=com.syswin.library.database.event.stream.DefaultStatefulTaskRunnerConfig",
    "spring.datasource.username=root",
    "spring.datasource.password=password"
})
public class EventHousekeeperTest {

  @ClassRule
  public static final MysqlContainer mysql = new MysqlContainer()
      .withNetworkAliases("mysql-temail")
      .withEnv("MYSQL_DATABASE", "consistency")
      .withEnv("MYSQL_USER", "syswin")
      .withEnv("MYSQL_PASSWORD", "password")
      .withEnv("MYSQL_ROOT_PASSWORD", "password");

  @MockBean
  private MQProducer mqProducer;

  @Autowired
  private ListenerEventRepo listenerEventRepo;

  @Autowired
  private DataSource dataSource;

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("spring.datasource.url",
        "jdbc:mysql://" + mysql.getContainerIpAddress() + ":" + mysql.getMappedPort(3306) + "/consistency?useSSL=false");
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("spring.datasource.url");
  }

  @Test
  public void batchDelete() {
    List<ListenerEvent> events = listenerEventRepo.findReadyToSend("bob");
    assertThat(events).hasSize(2);

    EventHousekeeper housekeeper = new EventHousekeeper(dataSource, 50);
    housekeeper.run();

    events = listenerEventRepo.findReadyToSend("bob");
    assertThat(events).isEmpty();
  }
}
