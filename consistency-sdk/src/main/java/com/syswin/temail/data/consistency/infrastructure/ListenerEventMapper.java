package com.syswin.temail.data.consistency.infrastructure;

import com.syswin.temail.data.consistency.domain.ListenerEvent;
import org.apache.ibatis.annotations.Insert;

public interface ListenerEventMapper {

  @Insert("insert into listener_event(status,content,topic,tag) values "
      + "(#{status},#{content},#{topic},#{tag})")
  Integer insert(ListenerEvent listenerEvent);
}
