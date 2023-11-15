package com.zx.quant.klineproxy.client.model;

import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * binance exchange info
 * @author flamhaze5946
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class BinanceSpotExchange extends BinanceExchange {
  private List<BinanceSpotExchangeFilter> exchangeFilters;
  private List<BinanceSpotSymbol> symbols;
}
