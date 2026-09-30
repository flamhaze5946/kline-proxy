package com.zx.quant.klineproxy.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import lombok.Data;

/**
 * event mini ticker 24hr event
 * @author flamhaze5946
 */
@Data
public class EventMiniTicker24HrEvent {
  @JsonProperty("e")
  protected String eventType;
  @JsonProperty("E")
  protected Long eventTime;
  @JsonProperty("s")
  protected String symbol;
  @JsonProperty("c")
  protected BigDecimal lastPrice;
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
}
