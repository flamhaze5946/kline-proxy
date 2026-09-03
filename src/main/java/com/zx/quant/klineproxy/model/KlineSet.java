package com.zx.quant.klineproxy.model;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import lombok.Data;

/**
 * kline set key
 * @author flamhaze5946
 */
@Data
public class KlineSet {

  private final KlineSetKey key;

  private ConcurrentSkipListMap<Long, Kline> klineMap = new ConcurrentSkipListMap<>();

  /**
   * Open times whose kline is FINAL: the websocket sent the closing update (x=true), the bar came
   * from the REST API as a closed candle, it was restored from disk, or it is a synthetic fill.
   * Bulk queries with closed_only=true block on the just-closed bar until it is in this set.
   */
  private final Set<Long> finalOpenTimes = ConcurrentHashMap.newKeySet();

  public boolean isFinal(long openTime) {
    return finalOpenTimes.contains(openTime);
  }

  /** @return true when the flag was newly set */
  public boolean markFinal(long openTime) {
    return finalOpenTimes.add(openTime);
  }

  public void dropFinalBefore(long firstOpenTime) {
    finalOpenTimes.removeIf(openTime -> openTime < firstOpenTime);
  }

  public KlineSet(KlineSetKey key) {
    this.key = key;
  }
}
