package com.zx.quant.klineproxy.service;

import com.zx.quant.klineproxy.model.persistence.PersistedKlineRow;
import java.util.List;

/**
 * kline persistence store
 * @author flamhaze5946
 */
public interface KlinePersistenceStore {

  List<PersistedKlineRow> loadRows(String service, String interval, String symbol, int maxStoreCount);

  void dumpRows(String service, String interval, String symbol, List<PersistedKlineRow> rows,
                int maxStoreCount, long currentTime);
}
