package com.syswin.temail.data.consistency.mysql.stream;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ZkBasedStatefulTaskRunnerTest {

  private static TestingServer zookeeper;

  private int counter = 0;
  private final Queue<Integer> values = new ConcurrentLinkedQueue<>();
  private final StatefulTask task = new StatefulTask() {

    private final AtomicBoolean isStopped = new AtomicBoolean(false);

    @Override
    public void start() {
      isStopped.set(false);
      while (!isStopped.get() && !Thread.currentThread().isInterrupted()) {
        try {
          values.add(counter++);
          Thread.sleep(10);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    @Override
    public void stop() {
      isStopped.set(true);
    }
  };

  private ZkBasedStatefulTaskRunner taskRunner1;
  private ZkBasedStatefulTaskRunner taskRunner2;

  @BeforeClass
  public static void beforeClass() throws Exception {
    zookeeper = new TestingServer(2181, true);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    zookeeper.close();
  }

  @Before
  public void setUp() throws Exception {
    taskRunner1 = new ZkBasedStatefulTaskRunner(zookeeper.getConnectString(), UUID.randomUUID().toString(), task);
    taskRunner2 = new ZkBasedStatefulTaskRunner(zookeeper.getConnectString(), UUID.randomUUID().toString(), task);

    taskRunner1.start();
    taskRunner2.start();
  }

  @After
  public void tearDown() {
    taskRunner1.shutdown();
    taskRunner2.shutdown();
  }

  @Test
  public void runTaskOnLeaderOnly() throws InterruptedException {
    Thread.sleep(500);

    ZkBasedStatefulTaskRunner leader = taskRunner1.isLeader() ? taskRunner1 : taskRunner2;
    leader.shutdown();
    int countProducedByLastLeader = values.size();
    Thread.sleep(500);

    // task continues on leader alive
    assertThat(values.size()).isGreaterThan(countProducedByLastLeader);

    // task executed on leader only
    Integer previous = values.poll();
    Integer current;
    while (!values.isEmpty()) {
      current = values.poll();
      assertThat(current - previous).isOne();
      previous = current;
    }
  }

  @Test
  public void resumeTaskOnLeaderOnly() throws Exception {
    Thread.sleep(200);

    zookeeper.stop();
    int countProducedByLastLeader = values.size();
    Thread.sleep(200);
    zookeeper.restart();
    Thread.sleep(200);

    // task resumes on reconnect
    assertThat(values.size()).isGreaterThan(countProducedByLastLeader);

    // task executed on leader only
    Integer previous = values.poll();
    Integer current;
    while (!values.isEmpty()) {
      current = values.poll();
      assertThat(current - previous).isOne();
      previous = current;
    }
  }

  @Test
  public void onlyOneTaskPerRunner() throws Exception {
    taskRunner2.shutdown();
    for (int i = 0; i < 10; i++) {
      taskRunner1.run();
    }

    Thread.sleep(200);
    assertThat(taskRunner1.taskCount()).isEqualTo(1);
  }
}
