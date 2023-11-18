package com.zx.quant.klineproxy.service;

import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Ticker;
import java.util.Collection;
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
   * update kline
   * @param symbol    symbol
   * @param interval  interval
   * @param kline     kline
   */
  void updateKline(String symbol, String interval, Kline kline);
}
