package com.syswin.temail.data.consistency.mysql.stream;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class EventHousekeeper implements Runnable {

  private final DataSource dataSource;
  private final int limit;

  EventHousekeeper(DataSource dataSource, int limit) {
    this.dataSource = dataSource;
    this.limit = limit;
  }

  @Override
  public void run() {
    log.debug("Deleting {} events for housekeeping", limit);
    int count = update("delete from listener_event order by id asc limit ?", limit);
    log.debug("Deleted {} events for housekeeping", count);
  }

  private int update(String sql, int limit) {
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement statement = connection.prepareStatement(sql)) {
        log.debug("Executing sql statement {} on data source {}", sql, connection.getMetaData().getURL());
        statement.setInt(1, limit);
        return statement.executeUpdate();
      }
    } catch (SQLException e) {
      throw new ConsistencyDatabaseConnectionException("Failed to execute sql: " + sql, e);
    }
  }
}
