package com.zx.quant.klineproxy.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * kline set key
 * @author flamhaze5946
 */
@Data
@AllArgsConstructor
@EqualsAndHashCode
public class KlineSetKey {

  private String symbol;

  private String interval;
}
