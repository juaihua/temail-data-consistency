package com.syswin.temail.data.consistency.domain;

public class SendingMQMessageException extends RuntimeException {

  public SendingMQMessageException(Exception e) {
    super(e);
  }

  public SendingMQMessageException(String message){
    super(message);
  }
}
