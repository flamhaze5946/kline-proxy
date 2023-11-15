package com.zx.quant.klineproxy.client.model;

import java.math.BigDecimal;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * binance future filter
 * @author flamhaze5946
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class BinanceFutureSymbolFilter extends BinanceSymbolFilter {

  private BigDecimal notional;

  private Integer multiplierDecimal;
}
