package com.zx.quant.klineproxy.client.ws.client;

import org.springframework.beans.factory.annotation.Value;

/**
 * binance future websocket client
 * @author flamhaze5946
 */
public class BinanceSpotWebSocketClient extends BinanceWebSocketClient implements WebSocketClient {

  @Value("${ws.client.binanceSpot.url:wss://stream.binance.com/ws}")
  private String websocketUrl;

  public BinanceSpotWebSocketClient(int clientNumber) {
    super(clientNumber);
  }

  @Override
  protected String getUrl() {
    return websocketUrl;
  }
}
