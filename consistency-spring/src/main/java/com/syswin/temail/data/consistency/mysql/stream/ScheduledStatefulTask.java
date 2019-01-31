package com.syswin.temail.data.consistency.mysql.stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ScheduledStatefulTask implements StatefulTask {

  private final ListenerEventRepo eventRepo;
  private final int limit;
  private final long sweepInterval;
  private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
  private volatile boolean started = false;

  ScheduledStatefulTask(ListenerEventRepo eventRepo, int limit, long sweepInterval) {
    this.eventRepo = eventRepo;
    this.limit = limit;
    this.sweepInterval = sweepInterval;
    start();
  }

  @Override
  public void start(Consumer<Throwable> errorHandler) {
    started = true;
  }

  @Override
  public void stop() {
    started = false;
  }

  private void start() {
    scheduledExecutor.scheduleWithFixedDelay(
        () -> {
          if (started) {
            log.debug("Deleting {} events for housekeeping", limit);
            try {
              eventRepo.batchDelete(limit);
              log.debug("Deleted {} events for housekeeping", limit);
            } catch (Exception e) {
              log.error("Failed to delete {} events during housekeeping", limit, e);
            }
          }
        },
        sweepInterval, sweepInterval, MILLISECONDS);
  }

  void shutdown() {
    scheduledExecutor.shutdownNow();
  }
}
