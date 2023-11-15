package com.zx.quant.klineproxy.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import lombok.Data;

/**
 * event kline
 * @author flamhaze5946
 */
@Data
public class EventKline {
  @JsonProperty("t")
  private Long openTime;
  @JsonProperty("T")
  private Long closeTime;
  @JsonProperty("s")
  private String symbol;
  @JsonProperty("i")
  private String interval;
  @JsonProperty("f")
  private Long openMatchId;
  @JsonProperty("L")
  private Long closeMatchId;
  @JsonProperty("o")
  private BigDecimal openPrice;
  @JsonProperty("c")
  private BigDecimal closePrice;
  @JsonProperty("h")
  private BigDecimal highPrice;
  @JsonProperty("l")
  private BigDecimal lowPrice;
  @JsonProperty("v")
  private BigDecimal volume;
  @JsonProperty("n")
  private Integer tradeNum;
  @JsonProperty("x")
  private Boolean closed;
  @JsonProperty("q")
  private BigDecimal quoteVolume;
  @JsonProperty("V")
  private BigDecimal activeBuyVolume;
  @JsonProperty("Q")
  private BigDecimal activeBuyQuoteVolume;
  @JsonProperty("B")
  private String ignore;
}
