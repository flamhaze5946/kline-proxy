package com.zx.quant.klineproxy.client.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * binance server time
 * @author flamhaze5946
 */
@AllArgsConstructor
@Getter
public class BinanceServerTime {
  private final Long serverTime;
}
