package com.syswin.temail.data.consistency.application;

import static com.syswin.temail.data.consistency.mysql.stream.DataSyncFeature.BINLOG;
import static java.util.Arrays.asList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ThreadPoolExecutor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.togglz.core.manager.FeatureManager;

public class ToggleListenerEventServiceTest {

  private final FeatureManager featureManager = Mockito.mock(FeatureManager.class);
  private final TaskService taskService = Mockito.mock(TaskService.class);
  private final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
  private final ToggleListenerEventService toggle = new ToggleListenerEventService(featureManager, BINLOG, taskService, executor);

  private final String topic1 = "topic1";
  private final String topic2 = "topic2";

  @Before
  public void setUp() {
    executor.setCorePoolSize(1);
    executor.setKeepAliveSeconds(1);
    executor.setMaxPoolSize(1);
    executor.setQueueCapacity(10);
    executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    executor.initialize();
  }

  @After
  public void tearDown() {
    executor.shutdown();
  }

  @Test
  public void runUnderlyingServiceOnlyWhenEnabled() {
    when(featureManager.isActive(BINLOG)).thenReturn(true);

    toggle.doTask(asList(topic1, topic2));
    verify(taskService).doSendingMessage(topic1);
    verify(taskService).doSendingMessage(topic2);
  }

  @Test
  public void disableUnderlyingServiceWhenOff() {
    when(featureManager.isActive(BINLOG)).thenReturn(false);

    toggle.doTask(asList(topic1, topic2));
    verify(taskService, never()).doSendingMessage(topic1);
    verify(taskService, never()).doSendingMessage(topic2);
  }
}
