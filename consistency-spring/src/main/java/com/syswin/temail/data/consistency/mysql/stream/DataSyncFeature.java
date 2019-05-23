package com.syswin.temail.data.consistency.mysql.stream;

import org.togglz.core.Feature;
import org.togglz.core.annotation.Label;

public enum DataSyncFeature implements Feature {

  @Label("Binlog syncing")
  BINLOG
}
