package com.syswin.temail.data.consistency.domain;

import java.sql.Timestamp;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ListenerEvent {
  private long id;
  private Enum status;
  private String content;
  private String topic;
  private String tag;
  private Timestamp insert_time;
  private Timestamp update_time;

  public String key(){
    return new StringBuilder().append(topic).append("%").append(tag).toString();
  }
}
