package com.zx.quant.klineproxy.client.model;

import java.math.BigDecimal;
import lombok.Data;

/**
 * binance future asset
 * @author flamhaze5946
 */
@Data
public class BinanceFutureAsset {

  private String asset;

  private Boolean marginAvailable;

  private BigDecimal autoAssetExchange;
}
