/*
 * MIT License
 *
 * Copyright (c) 2019 Syswin
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

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
