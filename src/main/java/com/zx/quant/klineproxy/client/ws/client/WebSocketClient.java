package com.zx.quant.klineproxy.client.ws.client;

import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

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

  List<String> getSubscribedTopics();

  default void subscribeTopic(String topic) {
    subscribeTopics(Collections.singletonList(topic));
  }

  void subscribeTopics(Collection<String> topics);

  default void unsubscribeTopic(String topic) {
    unsubscribeTopics(Collections.singletonList(topic));
  }

  void unsubscribeTopics(Collection<String> topics);

  default void sendMessage(String message) {
    sendData(new TextWebSocketFrame(message));
  }

  void sendData(WebSocketFrame frame);

  void ping();

  void pong();

  void onReceive(String message);

  default void onReceiveNoHandle() {}

  void addMessageHandler(Consumer<String> messageHandler);

  void addMessageTopicExtractorHandler(Function<String, String> messageTopicExtractor);
}
