package com.syswin.temail.data.consistency.infrastructure;

import com.syswin.temail.data.consistency.domain.ListenerEvent;
import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

@Mapper
public interface ListenerEventMapper {

  Integer insert(ListenerEvent listenerEvent);
}
