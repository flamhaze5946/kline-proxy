package com.zx.quant.klineproxy.client.ws.client;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;

/**
 * websocket client
 * @author flamhaze5946
 */
public interface WebSocketClient {

  String clientName();

  URI uri();

  void start();

  void connect();

  void reconnect();

  void close();

  boolean alive();

  default void subscribeTopic(String topic) {
    subscribeTopics(Collections.singletonList(topic));
  }

  void subscribeTopics(Collection<String> topics);

  default void unsubscribeTopic(String topic) {
    unsubscribeTopics(Collections.singletonList(topic));
  }

  void unsubscribeTopics(Collection<String> topics);

  void sendMessage(String message);

  void ping();

  void onReceive(String message);

  default void onReceiveNoHandle() {}

  void addMessageHandler(Consumer<String> messageHandler);
}
