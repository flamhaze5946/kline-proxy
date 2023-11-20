package com.zx.quant.klineproxy.service;

import java.util.List;

/**
 * exchange service
 * @author flamhaze5946
 */
public interface ExchangeService<T> {

  /**
   * query exchange
   * @return
   */
  T queryExchange();

  /**
   * get server time
   * @return server time
   */
  long queryServerTime();

  /**
   * query symbols
   * @return symbols
   */
  List<String> querySymbols();
}
