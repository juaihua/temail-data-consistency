package com.syswin.temail.data.consistency.mysql.stream;

import java.util.function.Consumer;

class CompositeStatefulTask implements StatefulTask {

  private final StatefulTask[] tasks;

  CompositeStatefulTask(StatefulTask... tasks) {
    this.tasks = tasks;
  }

  @Override
  public void start(Consumer<Throwable> errorHandler) {
    for (StatefulTask task : tasks) {
      task.start(errorHandler);
    }
  }

  @Override
  public void stop() {
    for (StatefulTask task : tasks) {
      task.stop();
    }
  }
}
