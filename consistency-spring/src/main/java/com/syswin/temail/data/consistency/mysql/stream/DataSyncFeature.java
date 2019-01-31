package com.syswin.temail.data.consistency.mysql.stream;

import org.togglz.core.Feature;
import org.togglz.core.annotation.EnabledByDefault;
import org.togglz.core.annotation.Label;

public enum DataSyncFeature implements Feature {
  @EnabledByDefault
  @Label("Database polling")
  POLL,

  @Label("Binlog syncing")
  BINLOG
}
