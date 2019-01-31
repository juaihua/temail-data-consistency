package com.syswin.temail.data.consistency.mysql.stream;

import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;

import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import org.junit.Test;
import org.mockito.Mockito;

public class EventHousekeeperTest {

  private final int limit = 100;
  private final ListenerEventRepo eventRepo = Mockito.mock(ListenerEventRepo.class);
  private final EventHousekeeper housekeeper = new EventHousekeeper(eventRepo, limit);

  @Test
  public void housekeepingWithRepo() {
    housekeeper.run();

    verify(eventRepo).batchDelete(limit);
  }
}
