package com.zx.quant.klineproxy.client.ws.task;

import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import lombok.extern.slf4j.Slf4j;

/**
 * monitor task
 * @author flamhaze5946
 */
@Slf4j
public class MonitorTask implements Runnable {

  private static final long CHECK_INTERVAL_MILLS = 20000L;

  private long startTime = System.currentTimeMillis();

  private final WebSocketClient client;

  public MonitorTask(WebSocketClient client) {
    this.client = client;
  }

  public void heartbeat() {
    this.startTime = System.currentTimeMillis();
  }

  @Override
  public void run() {
    if (System.currentTimeMillis() - this.startTime > CHECK_INTERVAL_MILLS) {
      try {
        log.info("{}ms not received messages from client {}, reconnect.", CHECK_INTERVAL_MILLS, client.clientName());
        client.reconnect();
      } catch (Exception e) {
        log.warn("client {} reconnect failed.", client.clientName(), e);
      } finally {
        log.info("client {} reconnect complete.", client.clientName());
      }
    }
  }
}
