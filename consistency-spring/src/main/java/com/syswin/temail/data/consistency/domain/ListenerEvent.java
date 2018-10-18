package com.syswin.temail.data.consistency.domain;

import java.sql.Timestamp;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@RequiredArgsConstructor
public class ListenerEvent {
  private long id;
  @NonNull
  private SendingStatus status;
  @NonNull
  private String content;
  @NonNull
  private String topic;
  @NonNull
  private String tag;
  private Timestamp insertTime;
  private Timestamp updateTime;

  public String key(){
    return new StringBuilder().append(topic).append("%").append(tag).toString();
  }
}
