package com.syswin.temail.data.consistency.application;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.togglz.core.Feature;
import org.togglz.core.manager.FeatureManager;

@Slf4j
public class ToggleListenerEventService extends ListenerEventService {
  private final FeatureManager featureManager;
  private final Feature feature;

  public ToggleListenerEventService(FeatureManager featureManager,
      Feature feature,
      TaskService taskService,
      ThreadPoolTaskExecutor taskExecutor) {

    super(taskService, taskExecutor);
    this.featureManager = featureManager;
    this.feature = feature;
  }

  @Override
  public void doTask(List<String> topics) {
    if (featureManager.isActive(feature)) {
      log.debug("Working on listener event task of topics {} with feature {}", topics, feature);
      super.doTask(topics);
      log.debug("Completed work on listener event task of topics {} with feature {}", topics, feature);
    }
  }
}
