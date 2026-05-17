package com.zx.quant.klineproxy.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * bulk klines request
 * @author flamhaze5946
 */
public record BulkKlinesRequest(
    String interval,
    Integer limit,
    @JsonProperty("closed_only") Boolean closedOnly,
    List<String> symbols
) {
}
