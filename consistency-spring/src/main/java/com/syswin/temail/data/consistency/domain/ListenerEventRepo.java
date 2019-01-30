package com.syswin.temail.data.consistency.domain;

import java.util.List;

public interface ListenerEventRepo {

  List<ListenerEvent> findReadyToSend(String topic);

  int delete(long id);

  void batchDelete(int limit);
}
