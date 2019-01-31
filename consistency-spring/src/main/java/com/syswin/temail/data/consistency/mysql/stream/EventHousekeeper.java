package com.syswin.temail.data.consistency.mysql.stream;

import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class EventHousekeeper implements Runnable {

  private final ListenerEventRepo eventRepo;
  private final int limit;

  EventHousekeeper(ListenerEventRepo eventRepo, int limit) {
    this.eventRepo = eventRepo;
    this.limit = limit;
  }

  @Override
  public void run() {
    log.debug("Deleting {} events for housekeeping", limit);
    eventRepo.batchDelete(limit);
    log.debug("Deleted {} events for housekeeping", limit);
  }
}
