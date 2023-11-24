package com.zx.quant.klineproxy.client.ws.client;

import com.zx.quant.klineproxy.model.constant.Constants;
import org.springframework.beans.factory.annotation.Value;

/**
 * binance future websocket client
 * @author flamhaze5946
 */
public class BinanceFutureWebSocketClient extends BinanceWebSocketClient implements WebSocketClient {

  @Value("${ws.client.binanceFuture.url:wss://fstream.binance.com/stream}")
  private String websocketUrl;

  public BinanceFutureWebSocketClient(int clientNumber) {
    super(clientNumber);
  }

  @Override
  protected int getMaxFramesPerSecond() {
    return 4;
  }

  @Override
  protected String globalFrameSendRateLimiter() {
    return Constants.BINANCE_FUTURE_GLOBAL_WS_FRAME_RATE_LIMITER_NAME;
  }

  @Override
  protected String getUrl() {
    return websocketUrl;
  }
}
