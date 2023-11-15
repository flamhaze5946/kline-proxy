package com.zx.quant.klineproxy.client.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.math.BigDecimal;
import java.util.List;
import lombok.Data;

/**
 * binance future symbol
 * @author flamhaze5946
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class BinanceFutureSymbol {

  private String symbol;

  private String pair;

  private String contractType;

  private Long deliveryDate;

  private Long onboardDate;

  private String status;

  private BigDecimal maintMarginPercent;

  private BigDecimal requiredMarginPercent;

  private String baseAsset;

  private String quoteAsset;

  private String marginAsset;

  private Integer pricePrecision;

  private Integer quantityPrecision;

  private Integer baseAssetPrecision;

  private Integer quotePrecision;

  private String underlyingType;

  private List<String> underlyingSubType;

  private Integer settlePlan;

  private BigDecimal triggerProtect;

  private List<BinanceFutureSymbolFilter> filters;

  private List<String> orderTypes;

  private List<String> timeInForce;

  private BigDecimal liquidationFee;

  private BigDecimal marketTakeBound;

  private Long maxMoveOrderLimit;
}
