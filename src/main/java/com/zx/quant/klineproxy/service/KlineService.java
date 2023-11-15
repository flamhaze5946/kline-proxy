package com.zx.quant.klineproxy.service;

import com.zx.quant.klineproxy.model.Kline;
import java.util.List;

/**
 * kline service
 *
 * @author flamhaze5946
 */
public interface KlineService {

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
