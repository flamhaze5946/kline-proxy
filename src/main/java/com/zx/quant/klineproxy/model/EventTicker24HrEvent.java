package com.zx.quant.klineproxy.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * event ticker 24hr event
 * @author flamhaze5946
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class EventTicker24HrEvent extends EventTicker24Hr {
  @JsonProperty("e")
  protected String eventType;
  @JsonProperty("E")
  protected String eventTime;
  @JsonProperty("s")
  protected String symbol;
}
