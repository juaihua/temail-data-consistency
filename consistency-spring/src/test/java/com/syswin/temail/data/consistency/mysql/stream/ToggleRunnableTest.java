package com.syswin.temail.data.consistency.mysql.stream;

import static com.syswin.temail.data.consistency.mysql.stream.DataSyncFeature.BINLOG;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.Mockito;
import org.togglz.core.manager.FeatureManager;

public class ToggleRunnableTest {

  private final FeatureManager featureManager = Mockito.mock(FeatureManager.class);
  private final Runnable runnable = Mockito.mock(Runnable.class);
  private final ToggleRunnable toggleRunnable = new ToggleRunnable(featureManager, BINLOG, runnable);

  @Test
  public void runUnderlyingWhenOn() {
    when(featureManager.isActive(BINLOG)).thenReturn(true);

    toggleRunnable.run();
    verify(runnable).run();
  }

  @Test
  public void skipUnderlyingWhenOff() {
    when(featureManager.isActive(BINLOG)).thenReturn(false);

    toggleRunnable.run();
    verify(runnable, never()).run();
  }
}
