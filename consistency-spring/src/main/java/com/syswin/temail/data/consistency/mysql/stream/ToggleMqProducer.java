package com.syswin.temail.data.consistency.mysql.stream;

import com.syswin.temail.data.consistency.application.MQProducer;
import java.io.UnsupportedEncodingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.togglz.core.Feature;
import org.togglz.core.manager.FeatureManager;

@Slf4j
public class ToggleMqProducer implements MQProducer {

  private final FeatureManager featureManager;
  private final Feature feature;
  private final MQProducer mqProducer;

  public ToggleMqProducer(FeatureManager featureManager,
      Feature feature,
      MQProducer mqProducer) {
    this.featureManager = featureManager;
    this.feature = feature;
    this.mqProducer = mqProducer;
  }

  @Override
  public void send(String body, String topic, String tags, String keys)
      throws UnsupportedEncodingException, InterruptedException, RemotingException, MQClientException, MQBrokerException {

    if (featureManager.isActive(feature)) {
      log.debug("Sending message of topic: {}, tag: {} with feature {}", topic, tags, feature);
      mqProducer.send(body, topic, tags, keys);
      log.debug("Sent message of topic: {}, tag: {} with feature {}", topic, tags, feature);
    }
  }
}
