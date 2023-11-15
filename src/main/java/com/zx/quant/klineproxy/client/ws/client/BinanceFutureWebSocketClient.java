package com.zx.quant.klineproxy.client.ws.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * binance future websocket client
 * @author flamhaze5946
 */
@Component
public class BinanceFutureWebSocketClient extends BinanceWebSocketClient implements WebSocketClient {

  @Value("${ws.client.binanceFuture.name:binanceFutureWsClient}")
  private String clientName;

  @Value("${ws.client.binanceFuture.url:wss://fstream.binance.com/ws}")
  private String websocketUrl;

  @Override
  protected String getUrl() {
    return websocketUrl;
  }

  @Override
  public String clientName() {
    return clientName;
  }
}
