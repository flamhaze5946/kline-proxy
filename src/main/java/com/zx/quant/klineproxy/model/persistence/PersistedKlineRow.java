package com.zx.quant.klineproxy.model.persistence;

import lombok.Data;

/**
 * persisted kline row
 * @author flamhaze5946
 */
@Data
public class PersistedKlineRow {

  private long openTime;

  private String openPrice;

  private String highPrice;

  private String lowPrice;

  private String closePrice;

  private String volume;

  private long closeTime;

  private String quoteVolume;

  private int tradeNum;

  private String activeBuyVolume;

  private String activeBuyQuoteVolume;
}
