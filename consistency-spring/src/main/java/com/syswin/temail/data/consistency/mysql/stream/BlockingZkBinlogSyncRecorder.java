package com.syswin.temail.data.consistency.mysql.stream;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

@Slf4j
public class BlockingZkBinlogSyncRecorder extends ZkBinlogSyncRecorder {

  BlockingZkBinlogSyncRecorder(String clusterName, CuratorFramework curator) {
    super(clusterName, curator);
  }

  @Override
  public void record(String filename, long position) {
    updatePositionToZk(filename, position);
  }

  @Override
  public void flush() {
  }
}
