package com.syswin.temail.data.consistency.configuration.datasource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class DataSourceConfigurer {

  @Autowired
  SystemConfig systemConfig;

  @Bean("dynamicDataSource")
  public DataSource dynamicDataSource() {
    DynamicRoutingDataSource dynamicRoutingDataSource = new DynamicRoutingDataSource();
    Map<Object, Object> dataSourceMap = new HashMap<>(4);
    List<HikariConfig> db = systemConfig.getDb();
    db.forEach(hikariConfig -> {
      DataSource dataSource = new HikariDataSource(hikariConfig);
      dataSourceMap.put(hikariConfig.getPoolName(), dataSource);
    });

    dynamicRoutingDataSource.setTargetDataSources(dataSourceMap);

    return dynamicRoutingDataSource;
  }

  @Bean
  public SqlSessionFactory sqlSessionFactory() throws Exception {
    SqlSessionFactoryBean sessionFactory = new SqlSessionFactoryBean();
    sessionFactory.setDataSource(dynamicDataSource());
    ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
    sessionFactory.setMapperLocations(resolver
        .getResources("classpath:mapper/*.xml"));
    sessionFactory.setConfigLocation(resolver.getResource("classpath:mybatis/mybatis-config.xml"));
    return sessionFactory.getObject();
  }

  @Bean
  public PlatformTransactionManager transactionManager() {
    return new DataSourceTransactionManager(dynamicDataSource());
  }
}

