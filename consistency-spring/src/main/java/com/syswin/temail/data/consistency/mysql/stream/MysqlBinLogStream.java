package com.syswin.temail.data.consistency.mysql.stream;

import static com.github.shyiko.mysql.binlog.event.EventType.EXT_WRITE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.TABLE_MAP;
import static com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY;
import static com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.NullEventDataDeserializer;
import com.syswin.temail.data.consistency.domain.ListenerEvent;
import com.syswin.temail.data.consistency.domain.SendingMQMessageException;
import com.syswin.temail.data.consistency.domain.SendingStatus;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class MysqlBinLogStream {

  private final BinaryLogClient client;
  private final String hostname;
  private final int port;
  private final BinlogSyncRecorder binlogSyncRecorder;

  MysqlBinLogStream(String hostname,
      int port,
      String username,
      String password,
      BinlogSyncRecorder binlogSyncRecorder) {

    this.hostname = hostname;
    this.port = port;
    this.binlogSyncRecorder = binlogSyncRecorder;
    this.client = new BinaryLogClient(hostname, port, username, password);
  }

  void start(EventHandler eventHandler, Consumer<Throwable> errorHandler, String[] tableNames) throws IOException {
    Set<String> tableNameSet = new HashSet<>();
    Collections.addAll(tableNameSet, tableNames);

    client.setBinlogFilename(binlogSyncRecorder.filename());
    client.setBinlogPosition(binlogSyncRecorder.position());
    client.setEventDeserializer(createEventDeserializerOf(TABLE_MAP, EXT_WRITE_ROWS));
    client.registerEventListener(replicationEventListener(eventHandler, errorHandler, tableNameSet));

    log.info("Connecting to Mysql at {}:{}", hostname, port);
    client.connect();
  }

  void stop() {
    try {
      client.disconnect();
      client.getEventListeners().forEach(client::unregisterEventListener);
      binlogSyncRecorder.flush();
      log.info("Disconnected from Mysql at {}:{}", hostname, port);
    } catch (IOException e) {
      log.warn("Failed to disconnect from MySql at {}:{}", hostname, port, e);
    }
  }

  private BinaryLogClient.EventListener replicationEventListener(
      EventHandler eventHandler,
      Consumer<Throwable> errorHandler,
      Set<String> tableNames) {

    log.debug("Registering event handler for database tables {}", tableNames);
    return new TableEventListener(eventHandler, errorHandler, tableNames);
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
    log.debug("Only interested events will be serialized: {}", includedEventTypes);

    for (EventType eventType : EventType.values()) {
      if (!includedEventTypes.contains(eventType)) {
        eventDeserializer.setEventDataDeserializer(eventType, nullEventDataDeserializer);
      }
    }

    return eventDeserializer;
  }

  // TODO: 2019/1/30 this class can be separated from binlog stream to reduce coupling, in case of future extension
  private class TableEventListener implements EventListener {

    private final EventHandler eventHandler;
    private final Consumer<Throwable> errorHandler;
    private final Set<String> tableNames;
    private TableMapEventData eventData;

    TableEventListener(EventHandler eventHandler, Consumer<Throwable> errorHandler, Set<String> tableNames) {
      this.eventHandler = eventHandler;
      this.errorHandler = errorHandler;
      this.tableNames = tableNames;
    }

    @Override
    public void onEvent(Event event) {
      log.debug("Received binlog event {}", event.getHeader().getEventType());
      if (event.getData() != null) {
        handleDeserializedEvent(event);
      }
      binlogSyncRecorder.record(client.getBinlogFilename(), client.getBinlogPosition());
    }

    private void handleDeserializedEvent(Event event) {
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
        handleEvent(eventHandler, errorHandler, listenerEvents);
      }

      eventData = null;
    }

    private void handleEvent(EventHandler eventHandler, Consumer<Throwable> errorHandler, List<ListenerEvent> listenerEvents) {
      try {
        eventHandler.handle(listenerEvents);
      } catch (SendingMQMessageException e) {
        errorHandler.accept(e);
        throw e;
      }
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
}
