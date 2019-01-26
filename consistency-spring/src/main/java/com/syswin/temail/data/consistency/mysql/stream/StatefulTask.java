package com.syswin.temail.data.consistency.mysql.stream;

public interface StatefulTask {

  void start();

  void stop();
}
