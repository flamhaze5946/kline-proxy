package com.zx.quant.klineproxy.client.model;

import lombok.Data;

/**
 * binance exchange filter
 * @author flamhaze5946
 */
@Data
public class BinanceSpotExchangeFilter {
  private String filterType;
  private Long maxNumOrders;
  private Long maxNumAlgoOrders;
  private Long maxNumIcebergOrders;
}
