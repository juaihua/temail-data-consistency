package com.syswin.temail.data.consistency.infrastructure;

import com.syswin.temail.data.consistency.domain.ListenerEvent;
import java.util.List;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface ListenerEventMapper {

  List<ListenerEvent> selectReadyToSend(@Param("topic") String topic);

  int deleteById(long id);

}
