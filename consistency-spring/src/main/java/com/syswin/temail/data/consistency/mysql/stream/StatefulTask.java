package com.syswin.temail.data.consistency.mysql.stream;

import java.util.function.Consumer;

public interface StatefulTask {

  void start(Consumer<Throwable> errorHandler);

  void stop();
}
