package com.zx.quant.klineproxy.client.ws.client;

import com.zx.quant.klineproxy.model.constant.Constants;
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
  protected int getMaxFramesPerSecond() {
    return 1;
  }

  @Override
  protected String globalFrameSendRateLimiter() {
    return Constants.BINANCE_SPOT_GLOBAL_WS_FRAME_RATE_LIMITER_NAME;
  }

  @Override
  protected String getUrl() {
    return websocketUrl;
  }
}
