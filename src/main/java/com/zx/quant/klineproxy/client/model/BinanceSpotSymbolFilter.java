package com.zx.quant.klineproxy.client.model;

import java.math.BigDecimal;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * binance symbol filter
 * @author flamhaze5946
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class BinanceSpotSymbolFilter extends BinanceSymbolFilter {

  private Long avgPriceMins;
  private BigDecimal bidMultiplierUp;
  private BigDecimal bidMultiplierDown;
  private BigDecimal askMultiplierUp;
  private BigDecimal askMultiplierDown;
  private BigDecimal minNotional;
  private BigDecimal maxNotional;
  private Boolean applyToMarket;
  private Boolean applyMinToMarket;
  private Boolean applyMaxToMarket;
  private Long maxNumOrders;
  private Long maxNumAlgoOrders;
  private Long maxNumIcebergOrders;
  private BigDecimal maxPosition;
  private BigDecimal minTrailingAboveDelta;
  private BigDecimal maxTrailingAboveDelta;
  private BigDecimal minTrailingBelowDelta;
  private BigDecimal maxTrailingBelowDelta;
}
