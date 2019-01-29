package com.syswin.temail.data.consistency.mysql.stream;

import static com.syswin.temail.data.consistency.mysql.stream.ZookeeperPaths.ZK_ROOT_PATH;
import static org.apache.curator.framework.recipes.leader.LeaderLatch.State.CLOSED;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;

@Slf4j
class ZkBasedStatefulTaskRunner {

  private static final String LEADER_LATCH_PATH_TEMPLATE = ZK_ROOT_PATH + "/%s/leader";
  private final String leaderLatchPath;
  private final CuratorFramework curator;
  private final Consumer<Throwable> errorHandler = errorHandler();
  private volatile LeaderLatch leaderLatch;
  private final ExecutorService executor = Executors.newSingleThreadExecutor();

  private final String participantId;
  private final StatefulTask task;

  ZkBasedStatefulTaskRunner(String clusterName, String participantId, StatefulTask task, CuratorFramework curator) {
    this.participantId = participantId;
    this.task = task;

    this.curator = curator;
    leaderLatchPath = String.format(LEADER_LATCH_PATH_TEMPLATE, clusterName);
    leaderLatch = new LeaderLatch(curator, leaderLatchPath, participantId);
  }

  void start() throws Exception {
    curator.create().orSetData().creatingParentsIfNeeded().forPath(leaderLatchPath);

    leaderLatch.start();
    curator.getConnectionStateListenable().addListener((client, newState) -> {
      if (newState.isConnected()) {
        log.info("Participant {} is connected to zookeeper {}",
            leaderLatch.getId(),
            curator.getZookeeperClient().getCurrentConnectionString());
      } else {
        log.error("Participant {} is disconnected from zookeeper {}",
            leaderLatch.getId(),
            curator.getZookeeperClient().getCurrentConnectionString());

        this.task.stop();
      }
    });
    run();
  }

  private void run() {
    executor.execute(() -> {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          log.info("Participant {} is waiting for leadership", leaderLatch.getId());
          leaderLatch.await();
          log.info("Participant {} is running with leadership", leaderLatch.getId());
          task.start(errorHandler);
        }
      } catch (InterruptedException | EOFException e) {
        log.warn("Failed to acquire leadership due to interruption", e);
      }
    });
  }

  void shutdown() {
    task.stop();
    executor.shutdownNow();
    releaseLeadership();
  }

  private void releaseLeadership() {
    try {
      if (!CLOSED.equals(leaderLatch.getState())) {
        leaderLatch.close();
        log.info("Participant {} released leadership", leaderLatch.getId());
      }
    } catch (IOException e) {
      log.warn("Failed to close leader latch of participant {}", leaderLatch.getId(), e);
    }
  }

  private Consumer<Throwable> errorHandler() {
    return ex -> {
      log.error("Unexpected exception when running task on participant {}", participantId, ex);
      task.stop();
      releaseLeadership();
      leaderLatch = new LeaderLatch(curator, leaderLatchPath, participantId);
      try {
        leaderLatch.start();
      } catch (Exception e) {
        // this shall not happen
        log.error("Failed to start leader latch of participant {}", participantId);
      }
    };
  }

  boolean isLeader() {
    return leaderLatch.hasLeadership();
  }

  int participantCount() throws Exception {
    return leaderLatch.getParticipants().size();
  }
}
