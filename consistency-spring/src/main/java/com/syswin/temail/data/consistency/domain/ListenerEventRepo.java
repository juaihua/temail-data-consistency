package com.syswin.temail.data.consistency.domain;

import java.util.List;

public interface ListenerEventRepo {

  List<ListenerEvent> findReadyToSend();

  int updateStatus(long id, Enum status);
}
