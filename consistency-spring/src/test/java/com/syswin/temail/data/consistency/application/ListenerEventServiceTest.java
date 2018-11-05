package com.syswin.temail.data.consistency.application;


import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import com.syswin.temail.data.consistency.domain.SendingStatus;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public class ListenerEventServiceTest {

  private final ListenerEventRepo listenerEventRepo = Mockito.mock(ListenerEventRepo.class);
  private final ThreadPoolTaskExecutor taskExecutor = Mockito.mock(ThreadPoolTaskExecutor.class);
  private final MQProducer mqProducer = Mockito.mock(MQProducer.class);
  private final TaskService taskService = new TaskService(taskExecutor,mqProducer,listenerEventRepo);
  private final List<ListenerEvent> listenerEvents = Arrays.asList(
      new ListenerEvent(new Random().nextLong(),SendingStatus.NEW,"test content1","groupmail","1",Timestamp.valueOf(
          LocalDateTime.now()),Timestamp.valueOf(LocalDateTime.now())),
      new ListenerEvent(new Random().nextLong(),SendingStatus.NEW,"test content1","groupmail","2",Timestamp.valueOf(
          LocalDateTime.now()),Timestamp.valueOf(LocalDateTime.now())),
      new ListenerEvent(new Random().nextLong(),SendingStatus.NEW,"test content1","groupmail","3",Timestamp.valueOf(
          LocalDateTime.now()),Timestamp.valueOf(LocalDateTime.now())),
      new ListenerEvent(new Random().nextLong(),SendingStatus.NEW,"test content1","groupmail","4",Timestamp.valueOf(
          LocalDateTime.now()),Timestamp.valueOf(LocalDateTime.now())),
      new ListenerEvent(new Random().nextLong(),SendingStatus.NEW,"test content","groupmail","5",Timestamp.valueOf(
          LocalDateTime.now()),Timestamp.valueOf(LocalDateTime.now()))
  );

  @Test
  public void shouldGroupByToAddr() throws Exception {
//    when(listenerEventRepo.findReadyToSend()).thenReturn(listenerEvents);
//    Map<String, List<ListenerEvent>> toBeSend = taskService.findToBeSend(dbName);
//    System.out.println(toBeSend);
//    assertThat(toBeSend).isNotNull().hasSize(5);
//    assertThat(toBeSend).containsKeys("groupmail%1","groupmail%2","groupmail%3","groupmail%4","groupmail%5");
  }
}