package com.syswin.temail.data.consistency.configuration;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@MapperScan("com.syswin.temail.data.consistency.infrastructure")
public class MybatisConfiguration {

}
