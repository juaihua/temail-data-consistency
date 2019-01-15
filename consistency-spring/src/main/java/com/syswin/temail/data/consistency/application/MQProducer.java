package com.syswin.temail.data.consistency.application;

import java.io.UnsupportedEncodingException;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.exception.RemotingException;

public interface MQProducer {

  void send(String body, String topic, String tags, String keys) throws UnsupportedEncodingException, InterruptedException, RemotingException, MQClientException, MQBrokerException;
}
