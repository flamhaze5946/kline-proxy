package com.zx.quant.klineproxy.client.model;

import com.fasterxml.jackson.annotation.JsonFormat;
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
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  private BigDecimal bidMultiplierUp;
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  private BigDecimal bidMultiplierDown;
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  private BigDecimal askMultiplierUp;
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  private BigDecimal askMultiplierDown;
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  private BigDecimal minNotional;
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  private BigDecimal maxNotional;
  private Boolean applyToMarket;
  private Boolean applyMinToMarket;
  private Boolean applyMaxToMarket;
  private Long maxNumOrders;
  private Long maxNumAlgoOrders;
  private Long maxNumIcebergOrders;
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  private BigDecimal maxPosition;
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  private BigDecimal minTrailingAboveDelta;
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  private BigDecimal maxTrailingAboveDelta;
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  private BigDecimal minTrailingBelowDelta;
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  private BigDecimal maxTrailingBelowDelta;
}
