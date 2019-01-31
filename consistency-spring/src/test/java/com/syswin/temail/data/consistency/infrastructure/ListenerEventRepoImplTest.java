package com.syswin.temail.data.consistency.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import com.syswin.temail.data.consistency.application.ListenerEventService;
import com.syswin.temail.data.consistency.application.MQProducer;
import com.syswin.temail.data.consistency.containers.MysqlContainer;
import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.Network;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = DbTestApplication.class, properties = {
    "spring.autoconfigure.exclude[0]=com.systoon.integration.spring.boot.disconf.DisconfAutoConfiguration",
    "spring.autoconfigure.exclude[1]=com.systoon.integration.spring.boot.disconf.context.config.ConfigurationPropertiesRebinderAutoConfiguration",
    "spring.autoconfigure.exclude[2]=com.systoon.integration.spring.boot.disconf.context.config.RefreshAutoConfiguration",
    "spring.datasource.username=root",
    "spring.datasource.password=password"
})
public class ListenerEventRepoImplTest {
  private static final Network NETWORK = Network.newNetwork();

  @ClassRule
  public static final MysqlContainer mysql = new MysqlContainer().withNetwork(NETWORK)
      .withNetworkAliases("mysql-temail")
      .withEnv("MYSQL_DATABASE", "consistency")
      .withEnv("MYSQL_USER", "syswin")
      .withEnv("MYSQL_PASSWORD", "password")
      .withEnv("MYSQL_ROOT_PASSWORD", "password");

  @MockBean
  private MQProducer mqProducer;

  @MockBean
  private ListenerEventService listenerEventService;

  @Autowired
  private ListenerEventRepo listenerEventRepo;

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

    listenerEventRepo.batchDelete(50);

    events = listenerEventRepo.findReadyToSend("bob");
    assertThat(events).isEmpty();
  }
}
