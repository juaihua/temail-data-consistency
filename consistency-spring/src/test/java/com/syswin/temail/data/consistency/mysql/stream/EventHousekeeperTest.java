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

import static org.assertj.core.api.Assertions.assertThat;

import com.syswin.temail.data.consistency.application.MQProducer;
import com.syswin.temail.data.consistency.containers.MysqlContainer;
import com.syswin.temail.data.consistency.infrastructure.DbTestApplication;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
  public void batchDelete() throws SQLException {
    expectRowsInDb("bob", true, 2);

    EventHousekeeper housekeeper = new EventHousekeeper(dataSource, 50);
    housekeeper.run();

    expectRowsInDb("bob", false, 0);
  }

  private void expectRowsInDb(String topic, boolean hasRows, int count) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement statement = connection.prepareStatement("select topic, tag, content from listener_event where topic = ?")) {
        statement.setString(1, topic);
        try (ResultSet resultSet = statement.executeQuery()) {
          assertThat(resultSet.last()).isEqualTo(hasRows);
          assertThat(resultSet.getRow()).isEqualTo(count);
        }
      }
    }
  }
}
