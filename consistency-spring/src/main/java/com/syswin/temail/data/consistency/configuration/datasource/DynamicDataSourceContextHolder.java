package com.syswin.temail.data.consistency.configuration.datasource;

public class DynamicDataSourceContextHolder {

  /**
   * Maintain variable for every thread, to avoid effect other thread
   */
  private static final ThreadLocal<String> CONTEXT_HOLDER = ThreadLocal.withInitial(DataSourceKey.write::name);

  /**
   * Use write data source.
   */
  public static void useMasterDataSource() {
    CONTEXT_HOLDER.set(DataSourceKey.write.name());
  }

  /**
   * Use slave data source.
   */
  public static void useSlaveDataSource() {
    CONTEXT_HOLDER.set(DataSourceKey.read.name());
  }

  public static void set(String name) {
    CONTEXT_HOLDER.set(name);
  }

  /**
   * Get current DataSource
   */
  public static String getDataSourceKey() {
    return CONTEXT_HOLDER.get();
  }

  /**
   * To set DataSource as default
   */
  public static void clearDataSourceKey() {
    CONTEXT_HOLDER.remove();
  }

}
