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
    Map<String, List<Object[]>> klines
) {
}
