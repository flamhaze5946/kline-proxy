package com.zx.quant.klineproxy.model.statistic;

import lombok.Data;

/**
 * yama 01 date symbol info
 * @author flamhaze5946
 */
@Data
public class Yama01Index {

  private long openTime;

  private int btcRank;

  private int totalRank;

  private float index;
}
