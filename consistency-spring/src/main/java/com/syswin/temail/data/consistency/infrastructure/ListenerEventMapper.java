package com.syswin.temail.data.consistency.infrastructure;

import com.syswin.temail.data.consistency.domain.ListenerEvent;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface ListenerEventMapper {

  List<ListenerEvent> selectReadyToSend(@Param("topic") String topic);

  int updateStatusById(@Param("id") long id,@Param("status") Enum status);

  void delete(@Param("condition") LocalDateTime condition);

}
