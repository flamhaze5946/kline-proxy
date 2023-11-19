package com.zx.quant.klineproxy.client.ws.client;

import org.springframework.beans.factory.annotation.Value;

/**
 * binance future websocket client
 * @author flamhaze5946
 */
public class BinanceFutureWebSocketClient extends BinanceWebSocketClient implements WebSocketClient {

  @Value("${ws.client.binanceFuture.url:wss://fstream.binance.com/ws}")
  private String websocketUrl;

  public BinanceFutureWebSocketClient(int clientNumber) {
    super(clientNumber);
  }

  @Override
  protected String getUrl() {
    return websocketUrl;
  }
}
