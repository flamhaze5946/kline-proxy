package com.zx.quant.klineproxy.service;

import com.zx.quant.klineproxy.model.FutureFundingRate;
import java.util.List;

/**
 * exchange service
 * @author flamhaze5946
 */
public interface FutureExchangeService<T> extends ExchangeService<T> {

  /**
   * query funding rates in all market
   * @return funding rates
   */
  List<FutureFundingRate> queryFundingRates();
}
