package com.syswin.temail.data.consistency.mysql.stream;

import org.togglz.core.Feature;
import org.togglz.core.manager.FeatureManager;

class ToggleRunnable implements Runnable {

  private final FeatureManager featureManager;
  private final Feature feature;
  private final Runnable runnable;

  ToggleRunnable(FeatureManager featureManager,
      Feature feature,
      Runnable runnable) {
    this.featureManager = featureManager;
    this.feature = feature;
    this.runnable = runnable;
  }

  @Override
  public void run() {
    if (featureManager.isActive(feature)) {
      runnable.run();
    }
  }
}
