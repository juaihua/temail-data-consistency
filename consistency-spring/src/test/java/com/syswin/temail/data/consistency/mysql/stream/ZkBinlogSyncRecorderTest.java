package com.syswin.temail.data.consistency.mysql.stream;

import static com.seanyinx.github.unit.scaffolding.Randomness.nextLong;
import static com.seanyinx.github.unit.scaffolding.Randomness.uniquify;
import static com.syswin.temail.data.consistency.mysql.stream.ZkBinlogSyncRecorder.BINLOG_POSITION_PATH;
import static com.syswin.temail.data.consistency.mysql.stream.ZkBinlogSyncRecorder.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ZkBinlogSyncRecorderTest {
  private static TestingServer zookeeper;
  private static CuratorFramework curator;

  private final String filename = uniquify("filename");
  private final long position = nextLong();

  private ZkBinlogSyncRecorder recorder;

  @BeforeClass
  public static void beforeClass() throws Exception {
    zookeeper = new TestingServer(2181, true);
    curator = CuratorFrameworkFactory.newClient(
        zookeeper.getConnectString(),
        new ExponentialBackoffRetry(1000, Integer.MAX_VALUE));

    curator.start();
    curator.blockUntilConnected();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    curator.close();
    zookeeper.close();
  }

  @Before
  public void setUp() throws Exception {
    recorder = new ZkBinlogSyncRecorder(zookeeper.getConnectString());
  }

  @After
  public void tearDown() {
    recorder.shutdown();
  }

  @Test
  public void recordBinlogPositionToZk() throws Exception {
    recorder.record(filename, position);

    assertThat(recorder.filename()).isEqualTo(filename);
    assertThat(recorder.position()).isEqualTo(position);

    byte[] bytes = curator.getData().forPath(BINLOG_POSITION_PATH);
    assertThat(new String(bytes)).isEqualTo(filename + SEPARATOR + position);
  }
}
