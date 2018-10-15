package com.syswin.temail.data.consistency.domain;

import org.springframework.context.ApplicationEvent;

public class TaskApplicationEvent extends ApplicationEvent {

  private static final long serialVersionUID = 1L;

  public TaskApplicationEvent(Object source) {
    super(source);
  }
}
