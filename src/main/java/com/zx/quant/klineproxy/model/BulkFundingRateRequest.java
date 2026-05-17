package com.zx.quant.klineproxy.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * bulk funding rate request
 * @author flamhaze5946
 */
public record BulkFundingRateRequest(
    List<String> symbols,
    @JsonProperty("since_ms") Long sinceMs,
    @JsonProperty("until_ms") Long untilMs,
    Integer limit
) {
}
