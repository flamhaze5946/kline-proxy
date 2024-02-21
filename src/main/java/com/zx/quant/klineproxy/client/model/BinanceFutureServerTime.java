package com.zx.quant.klineproxy.client.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * binance server time
 * @author flamhaze5946
 */
@EqualsAndHashCode(callSuper = true)
@Getter
public class BinanceFutureServerTime extends BinanceServerTime {
  public BinanceFutureServerTime(Long serverTime) {
    super(serverTime);
  }
}
