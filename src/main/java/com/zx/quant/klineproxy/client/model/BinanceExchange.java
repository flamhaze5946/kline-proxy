package com.zx.quant.klineproxy.client.model;

import java.util.List;
import lombok.Data;

/**
 * binance exchange info
 * @author flamhaze5946
 */
@Data
public class BinanceExchange {
  protected String timezone;
  protected Long serverTime;
  protected List<BinanceRateLimit> rateLimits;
}
