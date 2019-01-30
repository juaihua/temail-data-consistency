package com.syswin.temail.data.consistency.mysql.stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

class EventHouseKeeper {

  private final ListenerEventRepo eventRepo;
  private final int limit;
  private final long sweepInterval;
  private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

  EventHouseKeeper(ListenerEventRepo eventRepo, int limit, long sweepInterval) {
    this.eventRepo = eventRepo;
    this.limit = limit;
    this.sweepInterval = sweepInterval;
  }

  void start() {
    scheduledExecutor.scheduleWithFixedDelay(
        () -> eventRepo.batchDelete(limit),
        sweepInterval, sweepInterval, MILLISECONDS);
  }

  void shutdown() {
    scheduledExecutor.shutdownNow();
  }
}
