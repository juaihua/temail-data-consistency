package com.syswin.temail.data.consistency.mysql.stream;

public interface BinlogSyncRecorder {

  void record(String filename, long position);
}
