package com.syswin.temail.data.consistency.mysql.stream;

import com.syswin.temail.data.consistency.application.MQProducer;
import java.io.IOException;
import java.sql.SQLException;
import java.util.function.Consumer;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
class BinlogStreamConfig {

  @Bean(destroyMethod = "close")
  CuratorFramework curator(@Value("${app.consistency.binlog.zk.address}") String zookeeperAddress) throws InterruptedException {
    CuratorFramework curator = CuratorFrameworkFactory.newClient(
        zookeeperAddress,
        new ExponentialBackoffRetry(1000, Integer.MAX_VALUE));

    curator.start();
    log.info("Connecting to zookeeper at {}", zookeeperAddress);
    curator.blockUntilConnected();
    log.info("Connected to zookeeper at {}", zookeeperAddress);
    return curator;
  }

  @ConditionalOnProperty(value = "app.consistency.binlog.update.mode", havingValue = "async", matchIfMissing = true)
  @Bean
  BinlogSyncRecorder asyncBinlogSyncRecorder(CuratorFramework curator,
      @Value("${app.consistency.binlog.update.interval:500}") long updateIntervalMillis) {
    log.info("Starting with async binlog recorder");
    return new AsyncZkBinlogSyncRecorder(curator, updateIntervalMillis);
  }

  @ConditionalOnProperty(value = "app.consistency.binlog.update.mode", havingValue = "blocking")
  @Bean
  BinlogSyncRecorder blockingBinlogSyncRecorder(CuratorFramework curator) {
    log.info("Starting with blocking binlog recorder");
    return new BlockingZkBinlogSyncRecorder(curator);
  }

  @Bean
  EventHandler eventHandler(MQProducer mqProducer,
      @Value("${app.consistency.binlog.rocketmq.retry.interval:1000}") long retryIntervalMillis) {
    return new MqEventSender(mqProducer, 3, retryIntervalMillis);
  }

  @Bean
  StatefulTask binLogStreamTask(
      DataSource dataSource,
      @Value("${spring.datasource.username}") String username,
      @Value("${spring.datasource.password}") String password,
      @Value("${app.consistency.binlog.mysql.tables:listener_event}") String[] tableNames,
      EventHandler eventHandler,
      BinlogSyncRecorder binlogSyncRecorder) throws SQLException {

    String[] databaseUrl = dataSource.getConnection().getMetaData().getURL()
        .replaceFirst("^.*//", "")
        .replaceFirst("/.*$", "")
        .split(":");

    MysqlBinLogStream binLogStream = new MysqlBinLogStream(databaseUrl[0],
        Integer.parseInt(databaseUrl[1]),
        username,
        password,
        binlogSyncRecorder);

    return new StatefulTask() {
      @Override
      public void start(Consumer<Throwable> errorHandler) {
        try {
          binLogStream.start(eventHandler, errorHandler, tableNames);
        } catch (IOException e) {
          errorHandler.accept(e);
        }
      }

      @Override
      public void stop() {
        binLogStream.stop();
      }
    };
  }

  @Bean(initMethod = "start", destroyMethod = "shutdown")
  ZkBasedStatefulTaskRunner taskRunner(
      @Value("${app.consistency.binlog.participant.id}") String participantId,
      StatefulTask task,
      CuratorFramework curator) {
    return new ZkBasedStatefulTaskRunner(participantId, task, curator);
  }
}
