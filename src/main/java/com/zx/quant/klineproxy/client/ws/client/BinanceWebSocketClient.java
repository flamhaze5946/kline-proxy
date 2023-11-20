package com.zx.quant.klineproxy.client.ws.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
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

  public BinanceWebSocketClient() {
  }

  public BinanceWebSocketClient(int clientNumber) {
    super(clientNumber);
  }

  @Override
  protected int getMaxTopicsPerTime() {
    return 100;
  }

  protected void subscribeTopics0(Collection<String> topics, boolean subscribe) {
    if (!alive()) {
      log.warn("websocket client: {} not alived when subscribe topics.", clientName());
      return;
    }
    String method = subscribe ? SubscribeBody.SUBSCRIBE_METHOD : SubscribeBody.UNSUBSCRIBE_METHOD;
    SubscribeBody body = new SubscribeBody(method);
    body.setParams(new ArrayList<>(topics));
    body.setId(generateId());

    String dataJson = serializer.toJsonString(body);
    this.sendMessage(dataJson);

    if (subscribe) {
      this.topics.addAll(topics);
    } else {
      this.topics.removeAll(topics);
    }
  }

  @Override
  protected Integer generateSubId() {
    return new Random().nextInt(10000);
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
