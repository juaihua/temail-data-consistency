package com.syswin.temail.data.consistency.mysql.stream;

class ConsistencyDatabaseConnectionException extends RuntimeException {

  ConsistencyDatabaseConnectionException(String message, Throwable e) {
    super(message, e);
  }
}
