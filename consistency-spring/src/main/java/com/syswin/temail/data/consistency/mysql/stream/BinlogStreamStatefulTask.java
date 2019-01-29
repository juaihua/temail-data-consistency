package com.syswin.temail.data.consistency.mysql.stream;

import java.io.IOException;
import java.util.function.Consumer;

class BinlogStreamStatefulTask implements StatefulTask {

  private final MysqlBinLogStream binLogStream;
  private final EventHandler eventHandler;
  private final String[] tableNames;

  BinlogStreamStatefulTask(MysqlBinLogStream binLogStream, EventHandler eventHandler, String[] tableNames) {
    this.binLogStream = binLogStream;
    this.eventHandler = eventHandler;
    this.tableNames = tableNames;
  }

  @Override
  public void start(Consumer<Throwable> errorHandler) {
    try {
      binLogStream.start(eventHandler, errorHandler, tableNames);
    } catch (IOException e) {
      errorHandler.accept(e);
    }
  }

  @Override
  public void stop() {
    binLogStream.stop();
  }
}
