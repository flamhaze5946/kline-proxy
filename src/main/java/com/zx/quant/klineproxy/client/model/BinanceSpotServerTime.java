package com.zx.quant.klineproxy.client.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * binance server time
 * @author flamhaze5946
 */
@EqualsAndHashCode(callSuper = true)
@Getter
public class BinanceSpotServerTime extends BinanceServerTime {
  public BinanceSpotServerTime(Long serverTime) {
    super(serverTime);
  }
}
