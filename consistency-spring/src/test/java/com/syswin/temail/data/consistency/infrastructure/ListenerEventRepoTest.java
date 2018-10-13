package com.syswin.temail.data.consistency.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import com.syswin.temail.data.consistency.domain.SendingStatus;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Random;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles("test")
public class ListenerEventRepoTest {

  @Autowired
  private ListenerEventRepo listenerEventRepo;

  @Test
  public void save() throws Exception {
    ListenerEvent listenerEvent = new ListenerEvent(new Random().nextLong(),SendingStatus.NEW,"test content","groupmail","1",Timestamp.valueOf(
        LocalDateTime.now()),Timestamp.valueOf(LocalDateTime.now()));
    Integer count = listenerEventRepo.save(listenerEvent);
    assertThat(count).isEqualTo(1);
    assertThat(listenerEvent.getId()).isNotNull();
  }


  @Test
  public void statusShouldBeNew() throws Exception {
    List<ListenerEvent> events = listenerEventRepo.findReadyToSend();
    if(events.size()>0){
      events.forEach(x ->
          assertThat(x.getStatus()).isEqualTo(SendingStatus.NEW)
      );
    }
  }

  @Test
  public void readyToSendListLessThan100() throws Exception {
    List<ListenerEvent> events = listenerEventRepo.findReadyToSend();
    assertThat(events.size()).isLessThan(100);
  }

  @Test
  public void updateStatusToSendById() throws Exception {
    ListenerEvent listenerEvent = new ListenerEvent(new Random().nextLong(),SendingStatus.NEW,"test content","groupmail","1",Timestamp.valueOf(
        LocalDateTime.now()),Timestamp.valueOf(LocalDateTime.now()));
    listenerEventRepo.save(listenerEvent);
    Integer count = listenerEventRepo.updateStatus(listenerEvent.getId(), SendingStatus.SENDED);
    assertThat(count).isEqualTo(1);
  }
}
