package com.zx.quant.klineproxy.model.persistence;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;

/**
 * persisted kline shard file
 * @author flamhaze5946
 */
@Data
public class PersistedKlineShardFile {

  private int version = 1;

  private String service;

  private String interval;

  private String symbol;

  private String day;

  private List<PersistedKlineRow> rows = new ArrayList<>();
}
