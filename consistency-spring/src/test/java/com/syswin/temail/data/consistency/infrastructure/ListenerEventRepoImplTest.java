package com.syswin.temail.data.consistency.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import com.syswin.temail.data.consistency.application.ListenerEventService;
import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = DbTestApplication.class)
@ActiveProfiles("h2")
public class ListenerEventRepoImplTest {

  @MockBean
  private ListenerEventService listenerEventService;

  @Autowired
  private ListenerEventRepo listenerEventRepo;

  @Test
  public void batchDelete() {
    List<ListenerEvent> events = listenerEventRepo.findReadyToSend("bob");
    assertThat(events).hasSize(2);

    listenerEventRepo.batchDelete(50);

    events = listenerEventRepo.findReadyToSend("bob");
    assertThat(events).isEmpty();
  }
}
