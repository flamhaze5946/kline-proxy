package com.zx.quant.klineproxy.client.ws.task;

import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

/**
 * topic monitor task
 * @author flamhaze5946
 */
@Slf4j
public class TopicMonitorTask extends MonitorTask implements Runnable {

  private static final long CHECK_INTERVAL_MILLS = 1000L * 60 * 2;

  private final String topic;

  private final AtomicBoolean processOn = new AtomicBoolean(false);

  public TopicMonitorTask(WebSocketClient client, String topic) {
    super(client, 5);
    this.topic = topic;
  }

  public void start() {
    processOn.set(true);
    this.lastMessageTime = System.currentTimeMillis();
  }

  public void stop() {
    processOn.set(false);
  }

  @Override
  protected void overHeartBeat() {
    if (!processOn.get()) {
      return;
    }
    try {
      if (client.getChannelRegisteredTopics().contains(topic)) {
        return;
      }
      log.info("{}ms not received messages from client {} for topic: {}, resubscribe.", getCheckDurationMills(), client.clientName(), topic);
      client.subscribeTopic(topic);
    } catch (Exception e) {
      log.warn("client {} resubscribe topic {} failed.", client.clientName(), topic, e);
    }
  }

  @Override
  protected long getCheckDurationMills() {
    return CHECK_INTERVAL_MILLS;
  }
}
