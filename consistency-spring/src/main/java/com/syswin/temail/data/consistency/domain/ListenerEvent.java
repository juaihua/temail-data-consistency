package com.syswin.temail.data.consistency.domain;

import java.sql.Timestamp;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@RequiredArgsConstructor
public class ListenerEvent {
  private long id;
  @NonNull
  private SendingStatus status;
  @NonNull
  @ToString.Exclude
  private String content;
  @NonNull
  private String topic;
  @NonNull
  private String tag;
  private Timestamp createTime;
  private Timestamp updateTime;

  public String key(){
    return new StringBuilder().append(topic).append("%").append(tag).toString();
  }
}
