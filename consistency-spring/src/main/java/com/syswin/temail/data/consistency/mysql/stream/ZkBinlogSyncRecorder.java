package com.syswin.temail.data.consistency.mysql.stream;

import static com.syswin.temail.data.consistency.mysql.stream.ZookeeperPaths.ZK_ROOT_PATH;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

@Slf4j
public abstract class ZkBinlogSyncRecorder implements BinlogSyncRecorder {

  private static final String BINLOG_POSITION_PATH_TEMPLATE = ZK_ROOT_PATH + "/%s/position";
  private final String recordPath;
  static final String SEPARATOR = ",";
  private final CuratorFramework curator;

  ZkBinlogSyncRecorder(String clusterName, CuratorFramework curator) {
    this.curator = curator;
    this.recordPath = String.format(BINLOG_POSITION_PATH_TEMPLATE, clusterName);
  }

  void updatePositionToZk(String filename, long position) {
    try {
      log.debug("Updating binlog position [{},{}] to zookeeper", filename, position);
      curator.create().orSetData()
          .creatingParentsIfNeeded()
          .forPath(recordPath, (filename + SEPARATOR + position).getBytes());
      log.debug("Updated binlog position [{},{}] to zookeeper", filename, position);
    } catch (Exception e) {
      log.error("Failed to record binlog position {} {} to zookeeper {}",
          filename,
          position,
          curator.getZookeeperClient().getCurrentConnectionString());
    }
  }

  @Override
  public String filename() {
    try {
      if (curator.checkExists().forPath(recordPath) == null) {
        return null;
      }

      return binlogPositionString().split(SEPARATOR)[0];
    } catch (Exception e) {
      log.error("Failed to retrieve binlog position on zookeeper with path {}", recordPath, e);
      throw new IllegalStateException(e);
    }
  }

  @Override
  public long position() {
    try {
      if (curator.checkExists().forPath(recordPath) == null) {
        return 0L;
      }

      return Long.parseLong(binlogPositionString().split(SEPARATOR)[1]);
    } catch (Exception e) {
      log.error("Failed to retrieve binlog position on zookeeper with path {}", recordPath, e);
      throw new IllegalStateException(e);
    }
  }

  @Override
  public String recordPath() {
    return recordPath;
  }

  void start() {
  }

  void shutdown() {
  }

  private String binlogPositionString() throws Exception {
    return new String(curator.getData().forPath(recordPath));
  }
}
