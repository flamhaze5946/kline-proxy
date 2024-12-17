package com.zx.quant.klineproxy.model.statistic;

import lombok.Data;

/**
 * yama 01 date symbol info
 * @author flamhaze5946
 */
@Data
public class YamaDateSymbolInfo {

  private String symbol;

  private long openTime;

  private float pctChange;

  private float pctChangeSum;

  private float quoteVolumeSum;
}
