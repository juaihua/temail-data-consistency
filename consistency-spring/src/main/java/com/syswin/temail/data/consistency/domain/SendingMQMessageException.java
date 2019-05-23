package com.syswin.temail.data.consistency.domain;

public class SendingMQMessageException extends RuntimeException {

  public SendingMQMessageException(String message){
    super(message);
  }

  public SendingMQMessageException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
