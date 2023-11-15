package com.zx.quant.klineproxy.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * event kline
 * @author flamhaze5946
 */
@Data
public class EventKlineEvent {
  @JsonProperty("e")
  private String eventType;
  @JsonProperty("E")
  private String eventTime;
  @JsonProperty("s")
  private String symbol;
  @JsonProperty("k")
  private EventKline eventKline;
}
