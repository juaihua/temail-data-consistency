package com.syswin.temail.data.consistency.mysql.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.junit.Test;
import org.mockito.Mockito;

public class BinlogStreamStatefulTaskTest {

  private final List<Throwable> exceptions = new ArrayList<>();
  private final Consumer<Throwable> throwableConsumer = exceptions::add;

  private final EventHandler eventHandler = Mockito.mock(EventHandler.class);
  private final MysqlBinLogStream binLogStream = Mockito.mock(MysqlBinLogStream.class);
  private final String[] tableNames = {};

  private final BinlogStreamStatefulTask task = new BinlogStreamStatefulTask(binLogStream, eventHandler, tableNames);

  @Test
  public void startUnderlyingStream() throws IOException {

    task.start(throwableConsumer);

    assertThat(exceptions).isEmpty();
    verify(binLogStream).start(eventHandler, throwableConsumer, tableNames);
  }

  @Test
  public void stopUnderlyingStream() {

    task.stop();

    assertThat(exceptions).isEmpty();
    verify(binLogStream).stop();
  }

  @Test
  public void handleErrorOnException() throws IOException {
    IOException exception = new IOException("oops");
    doThrow(exception).when(binLogStream).start(eventHandler, throwableConsumer, tableNames);

    task.start(throwableConsumer);

    assertThat(exceptions).containsOnly(exception);
  }
}
