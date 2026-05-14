package com.zx.quant.klineproxy.model;

import com.zx.quant.klineproxy.util.ConvertUtil.DisplayFundingRate;
import java.util.List;
import java.util.Map;

public record BulkFundingRateResponse(
    long ts_ms,
    Map<String, List<DisplayFundingRate>> fundingRates
) {
}
