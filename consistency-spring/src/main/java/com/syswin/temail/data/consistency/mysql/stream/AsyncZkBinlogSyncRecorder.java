package com.syswin.temail.data.consistency.mysql.stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

@Slf4j
public class AsyncZkBinlogSyncRecorder extends ZkBinlogSyncRecorder {

  private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
  private final long updateIntervalMillis;
  private volatile String binlogFilePosition;

  AsyncZkBinlogSyncRecorder(CuratorFramework curator, long updateIntervalMillis) {
    super(curator);
    this.updateIntervalMillis = updateIntervalMillis;
  }

  @Override
  public void record(String filename, long position) {
    this.binlogFilePosition = filename + SEPARATOR + position;
    log.debug("Saved binlog position [{},{}] locally", binlogFilePosition);
  }

  @Override
  void start() {
    super.start();
    scheduledExecutor.scheduleWithFixedDelay(() -> {
      String[] strings = binlogFilePosition.split(SEPARATOR);
      updatePositionToZk(strings[0], Long.parseLong(strings[1]));
    }, updateIntervalMillis, updateIntervalMillis, MILLISECONDS);
  }

  @Override
  void shutdown() {
    super.shutdown();
    scheduledExecutor.shutdownNow();
  }
}
