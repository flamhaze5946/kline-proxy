package com.zx.quant.klineproxy.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * event kline
 * @author flamhaze5946
 */
@Data
public abstract class EventKline<N> {
  @JsonProperty("t")
  protected long openTime;
  @JsonProperty("T")
  protected long closeTime;
  @JsonProperty("s")
  protected String symbol;
  @JsonProperty("i")
  protected String interval;
  @JsonProperty("f")
  protected long openMatchId;
  @JsonProperty("L")
  protected long closeMatchId;
  @JsonProperty("o")
  protected N openPrice;
  @JsonProperty("c")
  protected N closePrice;
  @JsonProperty("h")
  protected N highPrice;
  @JsonProperty("l")
  protected N lowPrice;
  @JsonProperty("v")
  protected N volume;
  @JsonProperty("n")
  protected int tradeNum;
  @JsonProperty("x")
  protected boolean closed;
  @JsonProperty("q")
  protected N quoteVolume;
  @JsonProperty("V")
  protected N activeBuyVolume;
  @JsonProperty("Q")
  protected N activeBuyQuoteVolume;
  @JsonProperty("B")
  protected String ignore;

  @EqualsAndHashCode(callSuper = true)
  @NoArgsConstructor
  @Data
  public static class StringEventKline extends EventKline<String> {
  }

  @EqualsAndHashCode(callSuper = true)
  @NoArgsConstructor
  @Data
  public static class FloatEventKline extends EventKline<Float> {
  }

  @EqualsAndHashCode(callSuper = true)
  @NoArgsConstructor
  @Data
  public static class DoubleEventKline extends EventKline<Double> {
  }

  @EqualsAndHashCode(callSuper = true)
  @NoArgsConstructor
  @Data
  public static class BigDecimalEventKline extends EventKline<BigDecimal> {
  }
}
