package com.zx.quant.klineproxy.client.ws.client;

import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * binance websocket client
 * @author flamhaze5946
 */
@Slf4j
public abstract class BinanceWebSocketClient extends AbstractWebSocketClient<Integer> implements WebSocketClient {

  private final AtomicInteger idOffset = new AtomicInteger(0);

  public BinanceWebSocketClient(int clientNumber) {
    super(clientNumber);
  }

  @Override
  protected boolean monitorTopicMessage() {
    return true;
  }

  @Override
  protected int getMaxTopicsPerTime() {
    return 50;
  }

  protected WebSocketFrame buildSubscribeFrame(Collection<String> topics) {
    return buildSubscribeFrame(topics, SubscribeBody.SUBSCRIBE_METHOD);
  }

  protected WebSocketFrame buildUnsubscribeFrame(Collection<String> topics) {
    return buildSubscribeFrame(topics, SubscribeBody.UNSUBSCRIBE_METHOD);
  }

  protected WebSocketFrame buildSubscribeFrame(Collection<String> topics, String subscribeMethod) {
    SubscribeBody body = new SubscribeBody(subscribeMethod);
    body.setParams(new ArrayList<>(topics));
    body.setId(generateId());

    String dataJson = serializer.toJsonString(body);
    return new TextWebSocketFrame(dataJson);
  }

  @Override
  protected Integer generateSubId() {
    return clientNumber * (Integer.MAX_VALUE / 1000);
  }

  @Override
  protected Integer generateId() {
    return subId + idOffset.getAndIncrement();
  }

  @Data
  static class SubscribeBody {

    private static final String SUBSCRIBE_METHOD = "SUBSCRIBE";
    private static final String UNSUBSCRIBE_METHOD = "UNSUBSCRIBE";

    public SubscribeBody() {
      this(SUBSCRIBE_METHOD);
    }

    public SubscribeBody(String method) {
      this.method = method;
    }

    private final String method;

    private List<String> params;

    private Integer id;
  }
}
