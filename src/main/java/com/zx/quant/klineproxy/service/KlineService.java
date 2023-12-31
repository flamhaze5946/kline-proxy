package com.zx.quant.klineproxy.service;

import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Ticker;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * kline service
 *
 * @author flamhaze5946
 */
public interface KlineService {

  /**
   * query tickers
   * @param symbols symbols, not required
   * @return tickers
   */
  List<Ticker> queryTickers(Collection<String> symbols);

  /**
   * query klines
   * @param symbol    symbol
   * @param interval  interval
   * @param startTime startTime
   * @param endTime   endTime
   * @param limit     limit
   * @return klines
   */
  List<Kline> queryKlines(String symbol, String interval, Long startTime, Long endTime, Integer limit);

  /**
   * update klines
   * @param symbol    symbol
   * @param interval  interval
   * @param klines    klines
   */
  void updateKlines(String symbol, String interval, List<Kline> klines);

  /**
   * @see KlineService#updateKlines(String, String, List)
   */
  default void updateKline(String symbol, String interval, Kline kline) {
    updateKlines(symbol, interval, Collections.singletonList(kline));
  }
}
