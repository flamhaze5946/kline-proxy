package com.zx.quant.klineproxy.model.persistence;

import lombok.Data;

/**
 * persisted kline manifest
 * @author flamhaze5946
 */
@Data
public class PersistedKlineManifest {

  private int version = 1;

  private String service;

  private String interval;

  private String symbol;

  private Integer maxStoreCount;

  private Integer storedCount;

  private String boundaryDay;

  private String activeDay;

  private Long lastDumpedCleanOpenTime;

  private Long lastDumpedCleanCloseTime;
}
