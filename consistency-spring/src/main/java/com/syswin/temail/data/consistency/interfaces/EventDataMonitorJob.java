package com.syswin.temail.data.consistency.interfaces;

import com.syswin.temail.data.consistency.application.ListenerEventService;
import com.syswin.temail.data.consistency.application.MQProducer;
import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import com.syswin.temail.data.consistency.utils.JsonConverter;
import java.util.List;
import java.util.Map;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.quartz.QuartzJobBean;

@DisallowConcurrentExecution
public class EventDataMonitorJob extends QuartzJobBean {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventDataMonitorJob.class);
  private final ListenerEventService listenerEventService;

  private final ThreadPoolTaskExecutor taskExecutor;

  private final MQProducer mqProducer;

  private final ListenerEventRepo listenerEventRepo;

  private final JsonConverter<ListenerEvent> jsonConverter;

  @Autowired
  public EventDataMonitorJob(ListenerEventService listenerEventService, ThreadPoolTaskExecutor taskExecutor,
      MQProducer mqProducer, ListenerEventRepo listenerEventRepo, JsonConverter<ListenerEvent> jsonConverter) {
    this.listenerEventService = listenerEventService;
    this.taskExecutor = taskExecutor;
    this.mqProducer = mqProducer;
    this.listenerEventRepo = listenerEventRepo;
    this.jsonConverter = jsonConverter;
  }

  @Override
  protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
    Map<String, List<ListenerEvent>> sendMap = listenerEventService.findToBeSend();
    sendMap.forEach((k, v) -> {
      taskExecutor.execute(() -> {
        v.forEach(
            x -> {
              mqProducer.send(jsonConverter.toString(x));
              listenerEventRepo.updateStatus(x.getId(), "send");
              LOGGER.debug("RocketMQ send data=>[{}]", x);
            }
        );
      });
    });
  }

}
