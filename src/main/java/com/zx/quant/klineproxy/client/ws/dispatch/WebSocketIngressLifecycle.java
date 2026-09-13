package com.zx.quant.klineproxy.client.ws.dispatch;

import com.zx.quant.klineproxy.client.ws.client.AbstractWebSocketClient;
import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import java.time.Duration;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;

/** Stop producers and drain accepted frames before service destruction persists the final cache. */
@Slf4j
public final class WebSocketIngressLifecycle implements SmartLifecycle {
  private final List<WebSocketClient> clients;
  private final KlineMessageDispatcher dispatcher;
  private volatile boolean running;

  public WebSocketIngressLifecycle(List<WebSocketClient> clients, KlineMessageDispatcher dispatcher) {
    this.clients = List.copyOf(clients);
    this.dispatcher = dispatcher;
  }

  @Override
  public void start() {
    running = true; // clients are started by their owning service during initialization
  }

  @Override
  public synchronized void stop() {
    if (!running) {
      return;
    }
    for (WebSocketClient client : clients) {
      try {
        client.close();
      } catch (RuntimeException e) {
        log.error("Unable to stop WebSocket producer {} before ingress drain", client.clientName(), e);
      }
    }
    boolean genericDrained = AbstractWebSocketClient.awaitGenericMessageTasks(Duration.ofSeconds(30));
    boolean klinesDrained = dispatcher.shutdown(Duration.ofSeconds(30));
    if (!genericDrained || !klinesDrained) {
      log.error("WebSocket ingress shutdown incomplete: generic_drained={} klines_drained={} snapshot={}",
          genericDrained, klinesDrained, dispatcher.snapshot());
    } else {
      log.info("WebSocket ingress drained before persistence: {}", dispatcher.snapshot());
    }
    running = false;
  }

  @Override
  public boolean isRunning() {
    return running;
  }
}
