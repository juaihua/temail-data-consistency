package com.syswin.temail.data.consistency.domain;

import java.util.List;

public interface ListenerEventRepo {

  Integer save(ListenerEvent listenerEvent);

  List<ListenerEvent> findReadyToSend();

  Integer updateStatus(Long id, Enum status);
}
