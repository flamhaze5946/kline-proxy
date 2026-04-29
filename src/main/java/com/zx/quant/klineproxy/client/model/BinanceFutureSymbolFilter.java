package com.zx.quant.klineproxy.client.model;

import com.fasterxml.jackson.annotation.JsonFormat;
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

  @JsonFormat(shape = JsonFormat.Shape.STRING)
  private BigDecimal notional;

  private Integer multiplierDecimal;
}
