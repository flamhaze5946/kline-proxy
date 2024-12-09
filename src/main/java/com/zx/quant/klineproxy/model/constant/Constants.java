package com.zx.quant.klineproxy.model.constant;

import java.math.BigDecimal;

/**
 * constants
 * @author flamhaze5946
 */
public final class Constants {

  public static final BigDecimal EPS = new BigDecimal("0.00000001");

  public static final int SCALE = 8;

  public static final String BINANCE_SPOT_KLINES_FETCHER_RATE_LIMITER_NAME = "binanceSpotKlinesFetcherRateLimiter";

  public static final String BINANCE_SPOT_GLOBAL_WS_FRAME_RATE_LIMITER_NAME = "binanceSpotGlobalWsFrameRateLimiter";

  public static final String BINANCE_FUTURE_KLINES_FETCHER_RATE_LIMITER_NAME = "binanceFutureKlinesFetcherRateLimiter";

  public static final String BINANCE_FUTURE_GLOBAL_WS_FRAME_RATE_LIMITER_NAME = "binanceFutureGlobalWsFrameRateLimiter";

}
