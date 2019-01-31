package com.syswin.temail.data.consistency.mysql.stream;

import com.syswin.temail.data.consistency.application.MQProducer;
import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import java.sql.SQLException;
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

  @Value("${app.consistency.cluster.name}")
  private String clusterName;

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
  @Bean(initMethod = "start", destroyMethod = "shutdown")
  BinlogSyncRecorder asyncBinlogSyncRecorder(CuratorFramework curator,
      @Value("${app.consistency.binlog.update.interval:200}") long updateIntervalMillis) {
    log.info("Starting with async binlog recorder");
    return new AsyncZkBinlogSyncRecorder(clusterName, curator, updateIntervalMillis);
  }

  @ConditionalOnProperty(value = "app.consistency.binlog.update.mode", havingValue = "blocking")
  @Bean(initMethod = "start", destroyMethod = "shutdown")
  BinlogSyncRecorder blockingBinlogSyncRecorder(CuratorFramework curator) {
    log.info("Starting with blocking binlog recorder");
    return new BlockingZkBinlogSyncRecorder(clusterName, curator);
  }

  @Bean
  EventHandler eventHandler(MQProducer mqProducer,
      @Value("${app.consistency.binlog.rocketmq.retry.limit:3}") int maxRetries,
      @Value("${app.consistency.binlog.rocketmq.retry.interval:1000}") long retryIntervalMillis) {
    return new MqEventSender(mqProducer, maxRetries, retryIntervalMillis);
  }

  @Bean
  StatefulTask binLogStreamTask(
      DataSource dataSource,
      @Value("${spring.datasource.username}") String username,
      @Value("${spring.datasource.password}") String password,
      @Value("${app.consistency.binlog.mysql.tables:listener_event}") String[] tableNames,
      @Value("${app.consistency.binlog.housekeeper.sweep.limit:100}") int limit,
      @Value("${app.consistency.binlog.housekeeper.sweep.interval:5000}") long sweepInterval,
      ListenerEventRepo eventRepo,
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

    return new CompositeStatefulTask(
        new ScheduledStatefulTask(eventRepo, limit, sweepInterval),
        new BinlogStreamStatefulTask(binLogStream, eventHandler, tableNames));
  }
}
