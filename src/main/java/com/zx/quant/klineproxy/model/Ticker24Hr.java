package com.zx.quant.klineproxy.model;

import java.math.BigDecimal;
import lombok.Data;

/**
 * ticker 24hr
 * @author flamhaze5946
 */
@Data
public class Ticker24Hr {

  protected String symbol;

  protected BigDecimal priceChange;

  protected BigDecimal priceChangePercent;

  protected BigDecimal weightedAvgPrice;

  protected BigDecimal prevClosePrice;

  protected BigDecimal lastPrice;

  protected BigDecimal lastQty;

  protected BigDecimal bidPrice;

  protected BigDecimal bidQty;

  protected BigDecimal askPrice;

  protected BigDecimal askQty;

  protected BigDecimal openPrice;

  protected BigDecimal highPrice;

  protected BigDecimal lowPrice;

  protected BigDecimal volume;

  protected BigDecimal quoteVolume;

  protected Long openTime;

  protected Long closeTime;

  protected Long firstId;

  protected Long lastId;

  protected Long count;
}
