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

import static com.github.shyiko.mysql.binlog.event.EventType.EXT_WRITE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.TABLE_MAP;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.SendingStatus;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MysqlEventHandler implements Consumer<Event> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final EventHandler eventHandler;
  private final Set<String> tableNames;
  private TableMapEventData eventData;

  MysqlEventHandler(EventHandler eventHandler, String[] tableNames) {

    this.eventHandler = eventHandler;
    this.tableNames = new HashSet<>();
    Collections.addAll(this.tableNames, tableNames);
  }

  @Override
  public void accept(Event event) {
    if (TABLE_MAP.equals(event.getHeader().getEventType())) {
      TableMapEventData data = event.getData();
      if (tableNames.contains(data.getTable())) {
        log.debug("Processing binlog event: {}", event);
        eventData = data;
      }
    } else if (EXT_WRITE_ROWS.equals(event.getHeader().getEventType()) && eventData != null) {
      log.debug("Processing binlog event: {}", event);
      handleInsertEvent(event);
    }
  }

  private void handleInsertEvent(Event event) {
    WriteRowsEventData data = event.getData();

    if (data.getTableId() == eventData.getTableId()) {
      List<ListenerEvent> listenerEvents = data.getRows()
          .stream()
          .map(this::toListenerEvent)
          .collect(Collectors.toList());

      // listener events are sent in single element collections,
      // so it's safe to record binlog position once the collection of events is handled
      eventHandler.handle(listenerEvents);
    }

    eventData = null;
  }

  private ListenerEvent toListenerEvent(Serializable[] columns) {
    return new ListenerEvent(((long) columns[0]),
        SendingStatus.valueOf(new String((byte[]) columns[1]).toUpperCase()),
        new String((byte[]) columns[2]),
        new String((byte[]) columns[3]),
        new String((byte[]) columns[4]),
        Timestamp.from(Instant.ofEpochMilli(((long) columns[5]))),
        Timestamp.from(Instant.ofEpochMilli(((long) columns[6])))
    );
  }
}
