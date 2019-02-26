package com.syswin.temail.data.consistency.mysql.stream;

class ApplicationPaths {

  private static final String PARENT_PATH = "temail/consistency";

  static String clusterName(String clusterName) {
    return PARENT_PATH + "/" + clusterName;
  }
}
