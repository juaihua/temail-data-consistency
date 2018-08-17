package com.syswin.temail.data.consistency.infrastructure;

import static com.seanyinx.github.unit.scaffolding.Randomness.uniquify;
import static org.assertj.core.api.Assertions.assertThat;

import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
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
    ListenerEvent listenerEvent = new ListenerEvent(new Random().nextLong(),"test message");
    Integer count = listenerEventRepo.save(listenerEvent);
    assertThat(count).isEqualTo(1);
    assertThat(listenerEvent.getId()).isNotNull();
  }


  @Test
  public void statusShouldBeNew() throws Exception {
    List<ListenerEvent> events = listenerEventRepo.findReadyToSend();
    if(events.size()>0){
      events.forEach(x ->
          assertThat(x.getStatus()).isEqualTo("new")
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
    ListenerEvent listenerEvent = new ListenerEvent(uniquify("Jfw"),uniquify("Wqo"),"new",new Random().nextLong(),uniquify("content"));
    listenerEventRepo.save(listenerEvent);
    Integer count = listenerEventRepo.updateStatus(listenerEvent.getId(), "send");
    assertThat(count).isEqualTo(1);
  }
}
