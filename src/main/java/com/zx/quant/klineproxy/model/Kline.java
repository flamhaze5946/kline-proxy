package com.zx.quant.klineproxy.model;

import java.math.BigDecimal;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * kline
 * @author flamhaze5946
 */
@Data
public abstract class Kline<N> {

  protected Long openTime;

  protected Long closeTime;

  protected N openPrice;

  protected N highPrice;

  protected N lowPrice;

  protected N closePrice;

  protected N volume;

  protected N quoteVolume;

  protected Integer tradeNum;

  protected N activeBuyVolume;

  protected N activeBuyQuoteVolume;

  protected String ignore;

  @EqualsAndHashCode(callSuper = true)
  @NoArgsConstructor
  @Data
  public static class DoubleKline extends Kline<Double> {
  }


  @EqualsAndHashCode(callSuper = true)
  @NoArgsConstructor
  @Data
  public static class BigDecimalKline extends Kline<BigDecimal> {
  }

}
