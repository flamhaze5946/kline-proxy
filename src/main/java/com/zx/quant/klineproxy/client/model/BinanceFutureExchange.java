package com.zx.quant.klineproxy.client.model;

import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * binance future exchange
 * @author flamhaze5946
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class BinanceFutureExchange extends BinanceExchange {

  private String futuresType;

  private List<BinanceFutureExchangeFilter> exchangeFilters;

  private List<BinanceFutureAsset> assets;

  private List<BinanceFutureSymbol> symbols;

}
