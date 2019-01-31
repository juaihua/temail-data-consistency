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
  public int delete(long id) {
    return eventMapper.deleteById(id);
  }

  @Override
  public int batchDelete(int limit) {
    return eventMapper.batchDelete(limit);
  }
}
