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

import static com.syswin.library.messaging.all.spring.MqImplementation.ROCKET_MQ;
import static com.syswin.library.messaging.all.spring.MqImplementation.ROCKET_MQ_ONS;

import com.syswin.library.messaging.all.spring.MqProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class MqConfig {

  //'spring.rocketmqons.consistency.producer.group=GID_consistency-producer-group' needs to be added when using rocketmqons
  @Value("${spring.rocketmqons.consistency.producer.group:consistency-producer-group}")
  private String mqGroup;

  @ConditionalOnProperty(value = "library.messaging.rocketmq.enabled", havingValue = "true", matchIfMissing = true)
  @Bean
  MqProducerConfig rocketmqProducerConfig() {
    return new MqProducerConfig(mqGroup, ROCKET_MQ);
  }

  @ConditionalOnProperty(value = "library.messaging.rocketmqons.enabled", havingValue = "true")
  @Bean
  MqProducerConfig rocketmqOnsProducerConfig() {
    return new MqProducerConfig(mqGroup, ROCKET_MQ_ONS);
  }
}
