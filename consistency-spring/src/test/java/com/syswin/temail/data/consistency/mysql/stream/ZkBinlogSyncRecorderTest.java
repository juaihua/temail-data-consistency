package com.syswin.temail.data.consistency.mysql.stream;

import static com.seanyinx.github.unit.scaffolding.Randomness.nextLong;
import static com.seanyinx.github.unit.scaffolding.Randomness.uniquify;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ZkBinlogSyncRecorderTest {
  static CuratorFramework curator;
  private static TestingServer zookeeper;

  final String filename = uniquify("filename");
  final long position = nextLong();

  ZkBinlogSyncRecorder recorder;

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

  @After
  public void tearDown() {
    recorder.shutdown();
  }

  @Test(expected = IllegalStateException.class)
  public void blowsUpWhenGettingFileNameIfNotConnectedToZookeeper() {
    curator.close();
    recorder.filename();
  }

  @Test (expected = IllegalStateException.class)
  public void blowsUpWhenGettingPositionIfNotConnectedToZookeeper() {
    curator.close();
    recorder.position();
  }
}
