package com.syswin.temail.data.consistency.infrastructure;

import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.ListenerEventRepo;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class ListenerEventRepoImpl implements ListenerEventRepo {

  private final ListenerEventMapper eventMapper;

  @Autowired
  public ListenerEventRepoImpl(ListenerEventMapper eventMapper) {
    this.eventMapper = eventMapper;
  }

  @Override
  public List<ListenerEvent> findReadyToSend(String topic) {
    return eventMapper.selectReadyToSend(topic);
  }

  @Override
  public int updateStatus(long id, Enum status) {
    return eventMapper.updateStatusById(id,status);
  }
}
