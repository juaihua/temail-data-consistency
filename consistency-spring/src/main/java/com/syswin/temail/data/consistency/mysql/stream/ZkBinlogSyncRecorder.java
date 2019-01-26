package com.syswin.temail.data.consistency.mysql.stream;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

@Slf4j
public class ZkBinlogSyncRecorder implements BinlogSyncRecorder {

  static final String BINLOG_POSITION_PATH = "/syswin/temail/binlog_stream/position";
  static final String SEPARATOR = ",";

  private final CuratorFramework curator;
  private volatile String binlogFilePosition;
  private volatile String filename;
  private volatile long position;

  ZkBinlogSyncRecorder(String zookeeperAddress) throws InterruptedException {
    curator = CuratorFrameworkFactory.newClient(zookeeperAddress, new ExponentialBackoffRetry(1000, Integer.MAX_VALUE));
    curator.start();
    curator.blockUntilConnected();
  }

  // TODO: 2019/1/26 update to zk asynchronously
  @Override
  public void record(String filename, long position) {
    try {
      this.filename = filename;
      this.position = position;
      this.binlogFilePosition = filename + SEPARATOR + position;

      curator.create().orSetData()
          .creatingParentsIfNeeded()
          .forPath(BINLOG_POSITION_PATH, binlogFilePosition.getBytes());
    } catch (Exception e) {
      log.error("Failed to record binlog position {} {} to zookeeper {}",
          filename,
          position,
          curator.getZookeeperClient().getCurrentConnectionString());
    }
  }

  @Override
  public String filename() {
    return filename;
  }

  @Override
  public long position() {
    return position;
  }

  void shutdown() {
    curator.close();
  }
}
