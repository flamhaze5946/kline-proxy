package com.zx.quant.klineproxy.model;

import java.util.List;
import java.util.Map;

public record BulkKlinesResponse(
    String interval,
    long ts_ms,
    Map<String, List<Object[]>> klines
) {
}
