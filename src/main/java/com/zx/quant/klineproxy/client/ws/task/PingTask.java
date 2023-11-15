package com.zx.quant.klineproxy.client.ws.task;

import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import lombok.extern.slf4j.Slf4j;

/**
 * ping task
 * @author flamhaze5946
 */
@Slf4j
public class PingTask implements Runnable {

  private final WebSocketClient client;

  public PingTask(WebSocketClient client) {
    this.client = client;
  }

  @Override
  public void run() {
    this.client.ping();
  }
}
