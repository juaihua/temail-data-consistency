package com.syswin.temail.data.consistency.mysql.stream;

import static com.github.shyiko.mysql.binlog.event.EventType.EXT_WRITE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.TABLE_MAP;
import static com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY;
import static com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.NullEventDataDeserializer;
import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.SendingStatus;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MysqlBinLogStream {

  private static final long DATABASE_CONNECT_TIMEOUT = 2000L;

  private final BinaryLogClient client;
  private final String hostname;
  private final int port;
  private final BinlogSyncRecorder binlogSyncRecorder;
  private final EventHandler eventHandler;

  public MysqlBinLogStream(String hostname,
      int port,
      String username,
      String password,
      BinlogSyncRecorder binlogSyncRecorder,
      EventHandler eventHandler) {

    this.hostname = hostname;
    this.port = port;
    this.binlogSyncRecorder = binlogSyncRecorder;
    this.client = new BinaryLogClient(hostname, port, username, password);
    this.eventHandler = eventHandler;
  }

  public void start(String... tableNames) throws IOException, TimeoutException {
    Set<String> tableNameSet = new HashSet<>();
    Collections.addAll(tableNameSet, tableNames);

    client.setEventDeserializer(createEventDeserializerOf(TABLE_MAP, EXT_WRITE_ROWS));
    client.registerEventListener(replicationEventListener(tableNameSet));

    client.connect(DATABASE_CONNECT_TIMEOUT);
  }


  public void shutdown() {
    try {
      client.disconnect();
    } catch (IOException e) {
      log.warn("Failed to disconnect from MySql at {}:{}", hostname, port, e);
    }
  }

  private BinaryLogClient.EventListener replicationEventListener(Set<String> tableNames) {
    TableMapEventData[] eventData = new TableMapEventData[1];
    return event -> {
      if (event.getData() != null) {
        if (TABLE_MAP.equals(event.getHeader().getEventType())) {
          TableMapEventData data = event.getData();
          if (tableNames.contains(data.getTable())) {
            eventData[0] = data;
          }
        } else if (EXT_WRITE_ROWS.equals(event.getHeader().getEventType()) && eventData[0] != null) {
          WriteRowsEventData data = event.getData();

          if (data.getTableId() == eventData[0].getTableId()) {
            List<ListenerEvent> listenerEvents = data.getRows()
                .stream()
                .map(this::toListenerEvent)
                .collect(Collectors.toList());

            // listener events are sent in single element collections,
            // so it's safe to record binlog position once the collection of events is handled
            eventHandler.handle(listenerEvents);
            binlogSyncRecorder.record(client.getBinlogFilename(), client.getBinlogPosition());
          }

          eventData[0] = null;
        }
      }
    };
  }

  private EventDeserializer createEventDeserializerOf(EventType... includedTypes) {
    EventDeserializer eventDeserializer = new EventDeserializer();

    eventDeserializer.setCompatibilityMode(
        DATE_AND_TIME_AS_LONG,
        CHAR_AND_BINARY_AS_BYTE_ARRAY
    );

    EventDataDeserializer nullEventDataDeserializer = new NullEventDataDeserializer();

    Set<EventType> includedEventTypes = new HashSet<>();
    Collections.addAll(includedEventTypes, includedTypes);

    for (EventType eventType : EventType.values()) {
      if (!includedEventTypes.contains(eventType)) {
        eventDeserializer.setEventDataDeserializer(eventType, nullEventDataDeserializer);
      }
    }

    return eventDeserializer;
  }

  private ListenerEvent toListenerEvent(Serializable[] columns) {
    return new ListenerEvent(((long) columns[0]),
        SendingStatus.valueOf(new String((byte[]) columns[1])),
        new String((byte[]) columns[2]),
        new String((byte[]) columns[3]),
        new String((byte[]) columns[4]),
        Timestamp.from(Instant.ofEpochMilli(((long) columns[5]))),
        Timestamp.from(Instant.ofEpochMilli(((long) columns[6])))
    );
  }
}
