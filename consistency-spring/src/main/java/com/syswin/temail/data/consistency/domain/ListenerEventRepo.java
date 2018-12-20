package com.syswin.temail.data.consistency.domain;

import java.time.LocalDateTime;
import java.util.List;

public interface ListenerEventRepo {

  List<ListenerEvent> findReadyToSend(String topic);

  int updateStatus(long id, Enum status);

  void deleteByCondition(LocalDateTime condition);
}
