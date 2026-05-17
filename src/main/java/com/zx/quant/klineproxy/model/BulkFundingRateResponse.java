package com.zx.quant.klineproxy.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.zx.quant.klineproxy.util.ConvertUtil.DisplayFundingRate;
import java.util.List;
import java.util.Map;

/**
 * bulk funding rate response
 * @author flamhaze5946
 */
public record BulkFundingRateResponse(
    @JsonProperty("ts_ms") long tsMs,
    Map<String, List<DisplayFundingRate>> fundingRates
) {
}
