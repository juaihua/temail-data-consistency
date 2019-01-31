package com.syswin.temail.data.consistency.mysql.stream;

import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.togglz.core.manager.EnumBasedFeatureProvider;
import org.togglz.core.spi.FeatureProvider;

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
  FeatureProvider featureProvider() {
    return new EnumBasedFeatureProvider().addFeatureEnum(DataSyncFeature.class);
  }
}
