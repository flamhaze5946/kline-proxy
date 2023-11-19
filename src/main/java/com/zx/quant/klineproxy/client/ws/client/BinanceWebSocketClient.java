package com.zx.quant.klineproxy.client.ws.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;

/**
 * binance websocket client
 * @author flamhaze5946
 */
@Slf4j
public abstract class BinanceWebSocketClient extends AbstractWebSocketClient<Integer> implements WebSocketClient {

  private static final long BATCH_FUNC_WAIT_MILLS = 1000L;

  private static final int MAX_TOPICS = 100;

  private final AtomicInteger idOffset = new AtomicInteger(0);

  @Override
  public synchronized void subscribeTopics(Collection<String> topics) {
    partitionSubscribeTopics(topics, true);
  }

  @Override
  public synchronized void unsubscribeTopics(Collection<String> topics) {
    partitionSubscribeTopics(topics, false);
  }

  @Override
  protected Integer generateSubId() {
    return new Random().nextInt(10000);
  }

  @Override
  protected Integer generateId() {
    return subId + idOffset.getAndIncrement();
  }

  public void partitionSubscribeTopics(Collection<String> topics, boolean subscribe) {
    if (CollectionUtils.isEmpty(topics)) {
      return;
    }

    List<List<String>> topicsList = ListUtils.partition(new ArrayList<>(topics), MAX_TOPICS);
    for (List<String> subTopics : topicsList) {
      subscribeTopics0(subTopics, subscribe);
      try {
        Thread.sleep(BATCH_FUNC_WAIT_MILLS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void subscribeTopics0(Collection<String> topics, boolean subscribe) {
    if (!alive()) {
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
