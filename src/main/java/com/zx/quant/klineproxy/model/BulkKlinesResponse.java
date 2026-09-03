package com.zx.quant.klineproxy.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

/**
 * bulk klines response
 * @author flamhaze5946
 */
public record BulkKlinesResponse(
    String interval,
    @JsonProperty("ts_ms") long tsMs,
    Map<String, List<Object[]>> klines,
    /** every requested symbol's just-closed bar was final when this response was built */
    boolean finalized,
    /** symbols whose just-closed bar existed but was still not final (empty when finalized) */
    List<String> pending,
    @JsonProperty("waited_ms") long waitedMs,
    /**
     * requested symbols whose just-closed bar is still non-final but whose exchange status is not
     * TRADING (delisted / halted): not waited for, their last bar is returned as-is
     */
    @JsonProperty("not_trading") List<String> notTrading
) {

  public BulkKlinesResponse(String interval, long tsMs, Map<String, List<Object[]>> klines) {
    this(interval, tsMs, klines, true, List.of(), 0L, List.of());
  }

  public BulkKlinesResponse(String interval, long tsMs, Map<String, List<Object[]>> klines,
      boolean finalized, List<String> pending, long waitedMs) {
    this(interval, tsMs, klines, finalized, pending, waitedMs, List.of());
  }
}
