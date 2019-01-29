package com.syswin.temail.data.consistency.mysql.stream;

import static com.syswin.temail.data.consistency.mysql.stream.ZkBinlogSyncRecorder.SEPARATOR;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.junit.Before;
import org.junit.Test;

public class AsyncZkBinlogSyncRecorderTest extends ZkBinlogSyncRecorderTest {

  @Before
  public void setUp() throws Exception {
    super.setUp();
    recorder = new AsyncZkBinlogSyncRecorder(clusterName, curator, 300L);
  }

  @Test
  public void recordBinlogPositionToZk() {
    recorder.start();
    recorder.record(filename, position);

    await().atMost(1, SECONDS).ignoreExceptions().untilAsserted(() -> {
      byte[] bytes = curator.getData().forPath(recorder.recordPath());
      assertThat(new String(bytes)).isEqualTo(filename + SEPARATOR + position);
    });

    assertThat(recorder.filename()).isEqualTo(filename);
    assertThat(recorder.position()).isEqualTo(position);
  }
}
