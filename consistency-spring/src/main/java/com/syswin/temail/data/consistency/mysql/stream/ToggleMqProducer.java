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

import com.syswin.library.messaging.MessagingException;
import com.syswin.library.messaging.MqProducer;
import com.syswin.temail.data.consistency.application.MQProducer;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.togglz.core.Feature;
import org.togglz.core.manager.FeatureManager;

@Slf4j
public class ToggleMqProducer implements MQProducer {

  private final AtomicLong counter = new AtomicLong();
  private long previousCount = 0L;
  private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
  private final FeatureManager featureManager;
  private final Feature feature;
  private final Supplier<MqProducer> mqProducer;

  public ToggleMqProducer(FeatureManager featureManager,
      Feature feature,
      Supplier<MqProducer> mqProducer) {
    this.featureManager = featureManager;
    this.feature = feature;
    this.mqProducer = mqProducer;
  }

  @Override
  public void send(String body, String topic, String tags, String keys)
      throws UnsupportedEncodingException, InterruptedException, MessagingException {

    if (featureManager.isActive(feature)) {
      log.debug("Sending message of topic: {}, tag: {} with feature {}", topic, tags, feature);
      mqProducer.get().send(body, topic, tags, keys);
      log.debug("Sent message of topic: {}, tag: {} with feature {}", topic, tags, feature);
      counter.getAndIncrement();
    }
  }

  @Override
  public void start() {
    scheduledExecutor.scheduleWithFixedDelay(
        () -> {
          long count = counter.get();
          if (count > previousCount) {
            log.info("Sent {} messages since started", count);
            previousCount = count;
          }
        },
        30L, 30L, TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
    scheduledExecutor.shutdownNow();
  }
}
