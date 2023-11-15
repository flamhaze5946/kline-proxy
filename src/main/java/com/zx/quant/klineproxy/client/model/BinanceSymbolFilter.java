package com.zx.quant.klineproxy.client.model;

import java.math.BigDecimal;
import lombok.Data;

/**
 * binance symbol filter
 * @author flamhaze5946
 */
@Data
public class BinanceSymbolFilter {

  protected String filterType;
  protected BigDecimal minPrice;
  protected BigDecimal maxPrice;
  protected BigDecimal tickSize;
  protected BigDecimal minQty;
  protected BigDecimal maxQty;
  protected BigDecimal stepSize;
  protected Long limit;
  protected BigDecimal multiplierUp;
  protected BigDecimal multiplierDown;
}
