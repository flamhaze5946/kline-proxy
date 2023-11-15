package com.zx.quant.klineproxy.client.model;

import java.util.List;
import lombok.Data;

/**
 * binance symbol info
 * @author flamhaze5946
 */
@Data
public class BinanceSpotSymbol {
  private String symbol;
  private String status;
  private String baseAsset;
  private Integer baseAssetPrecision;
  private String quoteAsset;
  private Integer quoteAssetPrecision;
  private Integer quotePrecision;
  private Integer baseCommissionPrecision;
  private Integer quoteCommissionPrecision;
  private List<String> orderTypes;
  private Boolean icebergAllowed;
  private Boolean ocoAllowed;
  private Boolean quoteOrderQtyMarketAllowed;
  private Boolean allowTrailingStop;
  private Boolean isSpotTradingAllowed;
  private Boolean isMarginTradingAllowed;
  private Boolean cancelReplaceAllowed;
  private List<BinanceSpotSymbolFilter> filters;
  private List<String> permissions;
  private String defaultSelfTradePreventionMode;
  private List<String> allowedSelfTradePreventionModes;
}
