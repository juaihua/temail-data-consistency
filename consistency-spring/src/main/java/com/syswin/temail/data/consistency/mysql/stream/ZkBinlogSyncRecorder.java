package com.syswin.temail.data.consistency.mysql.stream;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

@Slf4j
public abstract class ZkBinlogSyncRecorder implements BinlogSyncRecorder {

  static final String BINLOG_POSITION_PATH = "/syswin/temail/binlog_stream/position";
  static final String SEPARATOR = ",";
  private final CuratorFramework curator;

  ZkBinlogSyncRecorder(CuratorFramework curator) {
    this.curator = curator;
  }

  void updatePositionToZk(String filename, long position) {
    try {
      curator.create().orSetData()
          .creatingParentsIfNeeded()
          .forPath(BINLOG_POSITION_PATH, (filename + SEPARATOR + position).getBytes());
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
      return binlogPositionString().split(SEPARATOR)[0];
    } catch (Exception e) {
      log.error("Failed to retrieve binlog position on zookeeper with path {}", BINLOG_POSITION_PATH, e);
      throw new IllegalStateException(e);
    }
  }

  @Override
  public long position() {
    try {
      return Long.parseLong(binlogPositionString().split(SEPARATOR)[1]);
    } catch (Exception e) {
      log.error("Failed to retrieve binlog position on zookeeper with path {}", BINLOG_POSITION_PATH, e);
      throw new IllegalStateException(e);
    }
  }

  void start() {
  }

  void shutdown() {
  }

  private String binlogPositionString() throws Exception {
    return new String(curator.getData().forPath(BINLOG_POSITION_PATH));
  }
}
