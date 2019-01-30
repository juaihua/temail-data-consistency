package com.syswin.temail.data.consistency.mysql.stream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;
import static org.mockito.Mockito.doAnswer;

import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class EventHouseKeeperTest {

  private final AtomicInteger deleteCount = new AtomicInteger();
  private final ListenerEventRepo eventRepo = Mockito.mock(ListenerEventRepo.class);

  private final int limit = 50;
  private final EventHouseKeeper houseKeeper = new EventHouseKeeper(eventRepo, limit, 100L);

  @Before
  public void setUp() {
    doAnswer(invocationOnMock -> deleteCount.getAndIncrement())
        .when(eventRepo)
        .batchDelete(limit);
  }

  @After
  public void tearDown() {
    houseKeeper.shutdown();
  }

  @Test
  public void deleteEventsPeriodically() {
    houseKeeper.start();

    waitAtMost(1, SECONDS).untilAsserted(() -> assertThat(deleteCount.get()).isGreaterThanOrEqualTo(1));
  }
}
