package com.syswin.temail.data.consistency.mysql.stream;

import com.syswin.temail.data.consistency.domain.ListenerEvent;
import java.util.List;

public interface EventHandler {

  void handle(List<ListenerEvent> listenerEvents);
}
