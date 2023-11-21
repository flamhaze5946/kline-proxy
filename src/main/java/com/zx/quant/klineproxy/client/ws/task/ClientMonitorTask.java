package com.zx.quant.klineproxy.client.ws.task;

import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import lombok.extern.slf4j.Slf4j;

/**
 * client monitor task
 * @author flamhaze5946
 */
@Slf4j
public class ClientMonitorTask extends MonitorTask implements Runnable {

  private static final long CHECK_INTERVAL_MILLS = 20000L;

  public ClientMonitorTask(WebSocketClient client) {
    super(client, 2);
  }

  @Override
  protected void overHeartBeat() {
    try {
      log.info("{}ms not received messages from client {}, reconnect.", getCheckDurationMills(), client.clientName());
      client.reconnect();
    } catch (Exception e) {
      log.warn("client {} reconnect failed.", client.clientName(), e);
    } finally {
      log.info("client {} reconnect complete.", client.clientName());
    }
  }

  @Override
  protected long getCheckDurationMills() {
    return CHECK_INTERVAL_MILLS;
  }
}
