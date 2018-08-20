package com.syswin.temail.data.consistency.domain;

import java.sql.Timestamp;

public class ListenerEvent {
  private Long id;
  private String fromAddr;
  private String toAddr;
  private String status;
  private Long contentId;
  private String content;
  private Timestamp insertTime;


  public ListenerEvent() {
  }

  public ListenerEvent(Long contentId, String content) {
    this.contentId = contentId;
    this.content = content;
  }

  public ListenerEvent(String fromAddr, String toAddr, String status, Long contentId, String content) {
    this.fromAddr = fromAddr;
    this.toAddr = toAddr;
    this.status = status;
    this.contentId = contentId;
    this.content = content;
  }

  public ListenerEvent(Long id, String fromAddr, String toAddr, String status, Long contentId, String content, Timestamp insertTime) {
    this.id = id;
    this.fromAddr = fromAddr;
    this.toAddr = toAddr;
    this.status = status;
    this.contentId = contentId;
    this.content = content;
    this.insertTime = insertTime;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getFromAddr() {
    return fromAddr;
  }

  public void setFromAddr(String fromAddr) {
    this.fromAddr = fromAddr;
  }

  public String getToAddr() {
    return toAddr;
  }

  public void setToAddr(String toAddr) {
    this.toAddr = toAddr;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public Long getContentId() {
    return contentId;
  }

  public void setContentId(Long contentId) {
    this.contentId = contentId;
  }

  public String getContent() {
    return content;
  }

  public void setContent(String content) {
    this.content = content;
  }

  public Timestamp getInsertTime() {
    return insertTime;
  }

  public void setInsertTime(Timestamp insertTime) {
    this.insertTime = insertTime;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ListenerEvent{");
    sb.append("id=").append(id);
    sb.append(", fromAddr='").append(fromAddr).append('\'');
    sb.append(", toAddr='").append(toAddr).append('\'');
    sb.append(", status='").append(status).append('\'');
    sb.append(", contentId=").append(contentId);
    sb.append(", content='").append(content).append('\'');
    sb.append(", insertTime=").append(insertTime);
    sb.append('}');
    return sb.toString();
  }
}
