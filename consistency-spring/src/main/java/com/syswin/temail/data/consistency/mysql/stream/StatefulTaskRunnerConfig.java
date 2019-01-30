package com.syswin.temail.data.consistency.mysql.stream;

import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@ConditionalOnProperty(value = "app.consistency.sync.mode", havingValue = "binlog")
@Configuration
class StatefulTaskRunnerConfig {

  @Bean(initMethod = "start", destroyMethod = "shutdown")
  ZkBasedStatefulTaskRunner taskRunner(
      @Value("${app.consistency.cluster.name}") String clusterName,
      @Value("${app.consistency.binlog.participant.id}") String participantId,
      StatefulTask task,
      CuratorFramework curator) {
    return new ZkBasedStatefulTaskRunner(clusterName, participantId, task, curator);
  }

  @Bean
  EventHouseKeeper houseKeeper(ListenerEventRepo eventRepo,
      @Value("${app.consistency.binlog.housekeeper.sweep.limit}") int limit,
      @Value("${app.consistency.binlog.housekeeper.sweep.Interval}") long sweepInterval) {

    return new EventHouseKeeper(eventRepo, limit, sweepInterval);
  }
}
