package com.zx.quant.klineproxy.client.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.math.BigDecimal;
import lombok.Data;

/**
 * binance symbol filter
 * @author flamhaze5946
 */
@Data
public class BinanceSymbolFilter {

  protected String filterType;
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  protected BigDecimal minPrice;
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  protected BigDecimal maxPrice;
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  protected BigDecimal tickSize;
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  protected BigDecimal minQty;
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  protected BigDecimal maxQty;
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  protected BigDecimal stepSize;
  protected Long limit;
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  protected BigDecimal multiplierUp;
  @JsonFormat(shape = JsonFormat.Shape.STRING)
  protected BigDecimal multiplierDown;
}
