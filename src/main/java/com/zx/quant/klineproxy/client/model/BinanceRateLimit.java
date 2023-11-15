package com.zx.quant.klineproxy.client.model;

import lombok.Data;

/**
 * binance rate limit
 * @author flamhaze5946
 */
@Data
public class BinanceRateLimit {
  private String rateLimitType;
  private String interval;
  private Long intervalNum;
  private Long limit;
}
