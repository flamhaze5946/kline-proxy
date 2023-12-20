package com.zx.quant.klineproxy.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.zx.quant.klineproxy.model.EventKline.BigDecimalEventKline;
import com.zx.quant.klineproxy.model.EventKline.DoubleEventKline;
import java.math.BigDecimal;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * event kline
 * @author flamhaze5946
 */
@Data
public class EventKlineEvent<N, EKN extends EventKline<N>> {
  @JsonProperty("e")
  protected String eventType;
  @JsonProperty("E")
  protected String eventTime;
  @JsonProperty("s")
  protected String symbol;
  @JsonProperty("k")
  protected EKN eventKline;

  @EqualsAndHashCode(callSuper = true)
  @NoArgsConstructor
  @Data
  public static class DoubleEventKlineEvent extends EventKlineEvent<Double, DoubleEventKline> {
  }

  @EqualsAndHashCode(callSuper = true)
  @NoArgsConstructor
  @Data
  public static class BigDecimalEventKlineEvent extends EventKlineEvent<BigDecimal, BigDecimalEventKline> {
  }
}
