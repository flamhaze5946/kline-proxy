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
public abstract class Kline {

  protected long openTime;

  protected long closeTime;

  protected int tradeNum;

  /*
  not use generic type to save memory
  protected N openPrice;

  protected N highPrice;

  protected N lowPrice;

  protected N closePrice;

  protected N volume;

  protected N quoteVolume;

  protected N activeBuyVolume;

  protected N activeBuyQuoteVolume;
  */

  /*
  always 0
  protected String ignore;
  */

  @EqualsAndHashCode(callSuper = true)
  @NoArgsConstructor
  @Data
  public static class StringKline extends Kline {

    protected String openPrice;

    protected String highPrice;

    protected String lowPrice;

    protected String closePrice;

    protected String volume;

    protected String quoteVolume;

    protected String activeBuyVolume;

    protected String activeBuyQuoteVolume;
  }

  @EqualsAndHashCode(callSuper = true)
  @NoArgsConstructor
  @Data
  public static class FloatKline extends Kline {

    protected float openPrice;

    protected float highPrice;

    protected float lowPrice;

    protected float closePrice;

    protected float volume;

    protected float quoteVolume;

    protected float activeBuyVolume;

    protected float activeBuyQuoteVolume;
  }

  @EqualsAndHashCode(callSuper = true)
  @NoArgsConstructor
  @Data
  public static class DoubleKline extends Kline {

    protected double openPrice;

    protected double highPrice;

    protected double lowPrice;

    protected double closePrice;

    protected double volume;

    protected double quoteVolume;

    protected double activeBuyVolume;

    protected double activeBuyQuoteVolume;
  }


  @EqualsAndHashCode(callSuper = true)
  @NoArgsConstructor
  @Data
  public static class BigDecimalKline extends Kline {

    protected BigDecimal openPrice;

    protected BigDecimal highPrice;

    protected BigDecimal lowPrice;

    protected BigDecimal closePrice;

    protected BigDecimal volume;

    protected BigDecimal quoteVolume;

    protected BigDecimal activeBuyVolume;

    protected BigDecimal activeBuyQuoteVolume;
  }
}
