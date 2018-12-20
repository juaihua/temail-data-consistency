package com.syswin.temail.data.consistency.interfaces;

import com.syswin.temail.data.consistency.application.HandleEventDataService;
import java.time.LocalDateTime;
import lombok.extern.slf4j.Slf4j;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;

@DisallowConcurrentExecution
@Slf4j
public class FlushHistoryDataJob extends QuartzJobBean {

  @Autowired
  private HandleEventDataService eventDataService;

  @Override
  protected void executeInternal(JobExecutionContext jobExecutionContext) throws JobExecutionException {
    log.info("start flush history data task : {}", LocalDateTime.now());
    eventDataService.flush();
  }
}
