package com.syswin.temail.data.consistency.mysql.stream;

import static com.syswin.temail.data.consistency.mysql.stream.ZkBinlogSyncRecorder.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public class BlockingZkBinlogSyncRecorderTest extends ZkBinlogSyncRecorderTest {

  @Before
  public void setUp() throws Exception {
    super.setUp();
    recorder = new BlockingZkBinlogSyncRecorder(clusterName, curator);
  }

  @Test
  public void recordBinlogPositionToZk() throws Exception {
    recorder.start();
    recorder.record(filename, position);

    assertThat(recorder.filename()).isEqualTo(filename);
    assertThat(recorder.position()).isEqualTo(position);

    byte[] bytes = curator.getData().forPath(recorder.recordPath());
    assertThat(new String(bytes)).isEqualTo(filename + SEPARATOR + position);
  }
}
