package com.syswin.temail.data.consistency.mysql.stream;

import static com.syswin.temail.data.consistency.mysql.stream.DataSyncFeature.BINLOG;
import static com.syswin.temail.data.consistency.mysql.stream.ApplicationPaths.clusterName;

import com.syswin.library.database.event.stream.mysql.MysqlBinlogStreamStatefulTaskBuilder;
import com.syswin.library.database.event.stream.zookeeper.AsyncZkBinlogSyncRecorder;
import com.syswin.library.database.event.stream.BinlogSyncRecorder;
import com.syswin.library.database.event.stream.zookeeper.BlockingZkBinlogSyncRecorder;
import com.syswin.library.database.event.stream.CounterBinlogSyncRecorder;
import com.syswin.library.stateful.task.runner.CompositeStatefulTask;
import com.syswin.library.stateful.task.runner.ScheduledStatefulTask;
import com.syswin.library.stateful.task.runner.StatefulTask;
import com.syswin.temail.data.consistency.application.MQProducer;
import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import java.sql.SQLException;
import java.util.Random;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.togglz.core.manager.FeatureManager;

@Slf4j
@Configuration
class BinlogStreamConfig {

  private final Random random = new Random(System.currentTimeMillis());
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
    return new CounterBinlogSyncRecorder(new AsyncZkBinlogSyncRecorder(clusterName(clusterName), curator, updateIntervalMillis));
  }

  @ConditionalOnProperty(value = "app.consistency.binlog.update.mode", havingValue = "blocking")
  @Bean(initMethod = "start", destroyMethod = "shutdown")
  BinlogSyncRecorder blockingBinlogSyncRecorder(CuratorFramework curator) {
    log.info("Starting with blocking binlog recorder");
    return new CounterBinlogSyncRecorder(new BlockingZkBinlogSyncRecorder(clusterName(clusterName), curator));
  }

  @Bean
  EventHandler eventHandler(MQProducer mqProducer,
      FeatureManager featureManager,
      @Value("${app.consistency.binlog.rocketmq.retry.limit:3}") int maxRetries,
      @Value("${app.consistency.binlog.rocketmq.retry.interval:1000}") long retryIntervalMillis) {
    return new MqEventSender(new ToggleMqProducer(featureManager, BINLOG, mqProducer), maxRetries, retryIntervalMillis);
  }

  @Bean
  StatefulTask binLogStreamTask(
      DataSource dataSource,
      @Value("${spring.datasource.username}") String username,
      @Value("${spring.datasource.password}") String password,
      @Value("${app.consistency.binlog.mysql.serverId:0}") long serverId,
      @Value("${app.consistency.binlog.mysql.tables:listener_event}") String[] tableNames,
      @Value("${app.consistency.binlog.housekeeper.sweep.limit:1000}") int limit,
      @Value("${app.consistency.binlog.housekeeper.sweep.interval:2000}") long sweepInterval,
      FeatureManager featureManager,
      ListenerEventRepo eventRepo,
      EventHandler eventHandler,
      BinlogSyncRecorder binlogSyncRecorder) throws SQLException {

    String[] databaseUrl = dataSource.getConnection().getMetaData().getURL()
        .replaceFirst("^.*//", "")
        .replaceFirst("/.*$", "")
        .split(":");

    serverId = serverId == 0 ? random.nextInt(Integer.MAX_VALUE) + 1 : serverId;
    StatefulTask binlogStreamStatefulTask = new MysqlBinlogStreamStatefulTaskBuilder()
        .hostname(databaseUrl[0])
        .port(Integer.parseInt(databaseUrl[1]))
        .username(username)
        .password(password)
        .serverId(serverId)
        .binlogSyncRecorder(binlogSyncRecorder)
        .databaseEventHandler(new MysqlEventHandler(eventHandler, tableNames))
        .build();

    return new CompositeStatefulTask(
        new ScheduledStatefulTask(new ToggleRunnable(featureManager, BINLOG, new EventHousekeeper(eventRepo, limit)), sweepInterval),
        binlogStreamStatefulTask);
  }
}
