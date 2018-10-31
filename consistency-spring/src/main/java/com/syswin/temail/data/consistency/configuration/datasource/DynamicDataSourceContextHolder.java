package com.syswin.temail.data.consistency.configuration.datasource;

public class DynamicDataSourceContextHolder {

  private static final ThreadLocal<String> CONTEXT_HOLDER = ThreadLocal.withInitial(()->"");

  public static void set(String name) {
    CONTEXT_HOLDER.set(name);
  }

  public static String getDataSourceKey() {
    return CONTEXT_HOLDER.get();
  }

  public static void clearDataSourceKey() {
    CONTEXT_HOLDER.remove();
  }

}
