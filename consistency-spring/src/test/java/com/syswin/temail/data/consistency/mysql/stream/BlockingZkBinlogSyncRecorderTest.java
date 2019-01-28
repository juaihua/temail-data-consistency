package com.syswin.temail.data.consistency.mysql.stream;

import static com.syswin.temail.data.consistency.mysql.stream.ZkBinlogSyncRecorder.BINLOG_POSITION_PATH;
import static com.syswin.temail.data.consistency.mysql.stream.ZkBinlogSyncRecorder.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public class BlockingZkBinlogSyncRecorderTest extends ZkBinlogSyncRecorderTest {

  @Before
  public void setUp() {
    recorder = new BlockingZkBinlogSyncRecorder(curator);
  }

  @Test
  public void recordBinlogPositionToZk() throws Exception {
    recorder.start();
    recorder.record(filename, position);

    assertThat(recorder.filename()).isEqualTo(filename);
    assertThat(recorder.position()).isEqualTo(position);

    byte[] bytes = curator.getData().forPath(BINLOG_POSITION_PATH);
    assertThat(new String(bytes)).isEqualTo(filename + SEPARATOR + position);
  }
}
