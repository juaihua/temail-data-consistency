package com.syswin.temail.data.consistency.application;



import static org.mockito.Mockito.when;
import static org.assertj.core.api.Assertions.assertThat;

import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.mockito.Mockito;

public class ListenerEventServiceTest {

  private final ListenerEventRepo listenerEventRepo = Mockito.mock(ListenerEventRepo.class);
  private final ListenerEventService  listenerEventService= new ListenerEventService(listenerEventRepo);
  private final List<ListenerEvent> listenerEvents = Arrays.asList(
      new ListenerEvent(1L,"bob","alice","new",1L,"test1", Timestamp.valueOf("2018-08-17 10:21:31")),
      new ListenerEvent(2L,"jack","alice","new",2L,"test2", Timestamp.valueOf("2018-08-17 10:22:01")),
      new ListenerEvent(3L,"bob","jack","new",3L,"test3", Timestamp.valueOf("2018-08-17 10:22:24")),
      new ListenerEvent(4L,"john","bob","new",4L,"test4", Timestamp.valueOf("2018-08-17 10:22:54")),
      new ListenerEvent(5L,"lucy","john","new",5L,"test5", Timestamp.valueOf("2018-08-17 10:23:25"))

  );

  @Test
  public void shouldGroupByToAddr() throws Exception {
    when(listenerEventRepo.findReadyToSend()).thenReturn(listenerEvents);
    Map<String, List<ListenerEvent>> toBeSend = listenerEventService.findToBeSend();
    System.out.println(toBeSend);
    assertThat(toBeSend).isNotNull().hasSize(4);
    assertThat(toBeSend).containsKeys("alice","jack","bob","john");
  }
}