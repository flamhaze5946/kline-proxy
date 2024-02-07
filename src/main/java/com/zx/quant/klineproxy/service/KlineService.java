package com.zx.quant.klineproxy.service;

import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Ticker;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;

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
  List<Ticker<?>> queryTickers(Collection<String> symbols);

  /**
   * @see KlineService#queryKlines(String, String, Long, Long, int, boolean)
   */
  default ImmutablePair<Collection<Kline<?>>, Integer> queryKlines(String symbol, String interval, Long startTime, Long endTime, int limit) {
    return queryKlines(symbol, interval, startTime, endTime, limit, false);
  }

  /**
   * @see KlineService#queryKlineList(String, String, Long, Long, int, boolean)
   */
  default List<Kline<?>> queryKlineList(String symbol, String interval, Long startTime, Long endTime, int limit) {
    return queryKlineList(symbol, interval, startTime, endTime, limit, false);
  }

  /**
   * @see KlineService#queryKlineArray(String, String, Long, Long, int, boolean)
   */
  default Kline<?>[] queryKlineArray(String symbol, String interval, Long startTime, Long endTime, int limit) {
    return queryKlineArray(symbol, interval, startTime, endTime, limit, false);
  }

  /**
   * @see KlineService#queryKlines(String, String, Long, Long, int, boolean)
   */
  default List<Kline<?>> queryKlineList(String symbol, String interval, Long startTime, Long endTime, int limit, boolean makeUp) {
    ImmutablePair<Collection<Kline<?>>, Integer> klinesPair = queryKlines(symbol, interval, startTime, endTime, limit, makeUp);
    ArrayList<Kline<?>> klineList = new ArrayList<>(klinesPair.getRight());
    klineList.addAll(klinesPair.getLeft());
    return klineList;
  }

  /**
   * @see KlineService#queryKlines(String, String, Long, Long, int, boolean)
   */
  default Kline<?>[] queryKlineArray(String symbol, String interval, Long startTime, Long endTime, int limit, boolean makeUp) {
    ImmutablePair<Collection<Kline<?>>, Integer> klinesPair = queryKlines(symbol, interval, startTime, endTime, limit, makeUp);
    Kline<?>[] klineArray = new Kline[klinesPair.getRight()];
    int index = 0;
    for (Kline<?> kline : klinesPair.getLeft()) {
      klineArray[index++] = kline;
    }
    return klineArray;
  }

  /**
   * query klines
   * @param symbol    symbol
   * @param interval  interval
   * @param startTime startTime
   * @param endTime   endTime
   * @param limit     limit
   * @param makeUp    make up klines
   * @return klines, klines size
   */
  ImmutablePair<Collection<Kline<?>>, Integer> queryKlines(String symbol, String interval, Long startTime, Long endTime, int limit, boolean makeUp);

  /**
   * update klines
   * @param symbol    symbol
   * @param interval  interval
   * @param klines    klines
   */
  void updateKlines(String symbol, String interval, List<Kline<?>> klines);

  /**
   * @see KlineService#updateKlines(String, String, List)
   */
  default void updateKline(String symbol, String interval, Kline<?> kline) {
    updateKlines(symbol, interval, Collections.singletonList(kline));
  }
}
