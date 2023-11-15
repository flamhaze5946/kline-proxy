package com.zx.quant.klineproxy.model;

import java.math.BigDecimal;
import lombok.Data;

/**
 * kline
 * @author flamhaze5946
 */
@Data
public class Kline {

  protected Long openTime;

  protected Long closeTime;

  protected BigDecimal openPrice;

  protected BigDecimal highPrice;

  protected BigDecimal lowPrice;

  protected BigDecimal closePrice;

  protected BigDecimal volume;

  protected BigDecimal quoteVolume;

  protected Integer tradeNum;

  protected BigDecimal activeBuyVolume;

  protected BigDecimal activeBuyQuoteVolume;

  protected String ignore;
}
