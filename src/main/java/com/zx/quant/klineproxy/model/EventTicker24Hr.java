package com.zx.quant.klineproxy.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import lombok.Data;

/**
 * event kline
 * @author flamhaze5946
 */
@Data
public class EventTicker24Hr {

  @JsonProperty("p")
  protected BigDecimal priceChange;

  @JsonProperty("P")
  protected BigDecimal priceChangePercent;

  @JsonProperty("w")
  protected BigDecimal weightedAvgPrice;

  @JsonProperty("x")
  protected BigDecimal prevClosePrice;

  @JsonProperty("c")
  protected BigDecimal lastPrice;

  @JsonProperty("Q")
  protected BigDecimal lastQty;

  @JsonProperty("b")
  protected BigDecimal bidPrice;

  @JsonProperty("B")
  protected BigDecimal bidQty;

  @JsonProperty("a")
  protected BigDecimal askPrice;

  @JsonProperty("A")
  protected BigDecimal askQty;

  @JsonProperty("o")
  protected BigDecimal openPrice;

  @JsonProperty("h")
  protected BigDecimal highPrice;

  @JsonProperty("l")
  protected BigDecimal lowPrice;

  @JsonProperty("v")
  protected BigDecimal volume;

  @JsonProperty("q")
  protected BigDecimal quoteVolume;

  @JsonProperty("O")
  protected Long openTime;

  @JsonProperty("C")
  protected Long closeTime;

  @JsonProperty("F")
  protected Long firstId;

  @JsonProperty("L")
  protected Long lastId;

  @JsonProperty("n")
  protected Long count;
}
