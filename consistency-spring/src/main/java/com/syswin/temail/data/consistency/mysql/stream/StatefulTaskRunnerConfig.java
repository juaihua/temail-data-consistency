package com.syswin.temail.data.consistency.mysql.stream;

import static com.syswin.temail.data.consistency.mysql.stream.ApplicationPaths.clusterName;

import com.syswin.library.stateful.task.runner.StatefulTask;
import com.syswin.library.stateful.task.runner.zookeeper.ZkBasedStatefulTaskRunner;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class StatefulTaskRunnerConfig {

  @Bean(initMethod = "start", destroyMethod = "shutdown")
  ZkBasedStatefulTaskRunner taskRunner(
      @Value("${app.consistency.cluster.name}") String clusterName,
      @Value("${app.consistency.binlog.participant.id}") String participantId,
      StatefulTask task,
      CuratorFramework curator) {
    return new ZkBasedStatefulTaskRunner(clusterName(clusterName), participantId, task, curator);
  }
}
