package com.zx.quant.klineproxy.client.ws.task;

import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

/**
 * monitor task
 * @author flamhaze5946
 */
@Slf4j
public abstract class MonitorTask implements Runnable {

  protected final WebSocketClient client;

  protected final int skipCheckTimes;

  protected final AtomicLong invokeTimes = new AtomicLong(0);

  protected long lastMessageTime = System.currentTimeMillis();

  protected MonitorTask(WebSocketClient client, int skipCheckTimes) {
    this.client = client;
    this.skipCheckTimes = skipCheckTimes;
  }

  protected MonitorTask(WebSocketClient client) {
    this(client, 0);
  }

  public void heartbeat() {
    this.lastMessageTime = System.currentTimeMillis();
  }

  @Override
  public void run() {
    if (System.currentTimeMillis() - this.lastMessageTime > getCheckDurationMills()) {
      synchronized (this) {
        if (System.currentTimeMillis() - this.lastMessageTime > getCheckDurationMills()) {
          long currentInvokeTimes = invokeTimes.incrementAndGet();
          if (currentInvokeTimes <= skipCheckTimes) {
            heartbeat();
            if (log.isDebugEnabled()) {
              log.debug("monitor: {} skip check in {}/{} times.", getClass().getSimpleName(), currentInvokeTimes, skipCheckTimes);
            }
            return;
          }

          overHeartBeat();
          heartbeat();
        }
      }
    }
  }

  protected abstract void overHeartBeat();

  protected abstract long getCheckDurationMills();
}
