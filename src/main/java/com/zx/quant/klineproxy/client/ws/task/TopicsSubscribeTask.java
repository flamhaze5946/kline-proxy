package com.zx.quant.klineproxy.client.ws.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

/**
 * topics subscribe task
 * @author flamhaze5946
 */
@Slf4j
public class TopicsSubscribeTask implements Runnable {

  private static final String METHOD = "subscribe";

  private final Queue<String> topicsQueue;

  private final int maxTopicsPerTime;

  private final Consumer<List<String>> subscribe0;

  public TopicsSubscribeTask(Queue<String> topicsQueue, int maxTopicsPerTime, Consumer<List<String>> subscribe0) {
    this.topicsQueue = topicsQueue;
    this.maxTopicsPerTime = maxTopicsPerTime;
    this.subscribe0 = subscribe0;
  }

  @Override
  public void run() {
    List<String> topics = new ArrayList<>(maxTopicsPerTime);
    try {
      if (CollectionUtils.isEmpty(topicsQueue)) {
        return;
      }

      while (!topicsQueue.isEmpty() && topics.size() < maxTopicsPerTime) {
        topics.add(topicsQueue.poll());
      }
      if (CollectionUtils.isNotEmpty(topics)) {
        subscribe0.accept(topics);
      }

    } catch (Exception e) {
      topicsQueue.addAll(topics);
      log.warn("{} topics failed, add them to queue again.", method());
    }
  }

  protected String method() {
    return METHOD;
  }
}
