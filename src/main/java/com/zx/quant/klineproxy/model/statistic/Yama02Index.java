package com.zx.quant.klineproxy.model.statistic;

import lombok.Data;

/**
 * yama 01 date symbol info
 * @author flamhaze5946
 */
@Data
public class Yama02Index {

  private long openTime;

  private float pctChangeSum;

  private int symbolCount;

  private float index;
}
