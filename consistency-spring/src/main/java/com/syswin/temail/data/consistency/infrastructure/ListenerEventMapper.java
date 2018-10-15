package com.syswin.temail.data.consistency.infrastructure;

import com.syswin.temail.data.consistency.domain.ListenerEvent;
import java.util.List;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface ListenerEventMapper {

  Integer insert(ListenerEvent listenerEvent);

  List<ListenerEvent> selectReadyToSend();

  int updateStatusById(@Param("id") long id,@Param("status") Enum status);
}
