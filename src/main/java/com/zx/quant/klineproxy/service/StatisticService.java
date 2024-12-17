package com.zx.quant.klineproxy.service;

import com.zx.quant.klineproxy.model.statistic.Yama01Index;
import com.zx.quant.klineproxy.model.statistic.Yama02Index;
import java.util.Map;

/**
 * statistic service
 * @author flamhaze5946
 */
public interface StatisticService {

  /**
   * record atr
   */
  void recordAtr();

  Map<String, Float> getAltCoinIndex();

  Map<Long, Yama01Index> getYama01AltCoinIndex();

  Map<Long, Yama02Index> getYama02AltCoinIndex();
}
