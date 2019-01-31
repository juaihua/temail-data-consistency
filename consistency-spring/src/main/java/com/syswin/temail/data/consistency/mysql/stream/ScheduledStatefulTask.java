package com.syswin.temail.data.consistency.mysql.stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ScheduledStatefulTask implements StatefulTask {

  private final long scheduledInterval;
  private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
  private final Runnable runnable;
  private volatile boolean started = false;

  ScheduledStatefulTask(Runnable runnable, long scheduledInterval) {
    this.scheduledInterval = scheduledInterval;
    this.runnable = runnable;
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
            try {
              runnable.run();
            } catch (Exception e) {
              log.error("Failed to run scheduled stateful task", e);
            }
          }
        },
        scheduledInterval, scheduledInterval, MILLISECONDS);
  }

  void shutdown() {
    scheduledExecutor.shutdownNow();
  }
}
