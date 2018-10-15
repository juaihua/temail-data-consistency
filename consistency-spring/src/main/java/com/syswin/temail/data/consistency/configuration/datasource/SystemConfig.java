package com.syswin.temail.data.consistency.configuration.datasource;

import com.zaxxer.hikari.HikariConfig;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix="spring")
public class SystemConfig {

  List<HikariConfig> db = new ArrayList<HikariConfig>();

  public List<HikariConfig> getDb() {
    return db;
  }

  public void setDb(List<HikariConfig> db) {
    this.db = db;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("SystemConfig{");
    sb.append("db=").append(db);
    sb.append('}');
    return sb.toString();
  }
}
