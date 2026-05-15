package com.zx.quant.klineproxy.model;

import java.util.List;

/**
 * Request body for {@code POST /fapi/v1/fundingRate/bulk}.
 *
 * <p>Mirrors the GET endpoint's query parameters, but accepts a JSON
 * body so callers can supply long symbol lists without URL length
 * limits.
 *
 * <p>Null/empty {@code symbols} preserves the existing GET behavior:
 * route through the chunk-cache for all symbols in the window.
 */
public record BulkFundingRateRequest(
    List<String> symbols,
    Long since_ms,
    Long until_ms,
    Integer limit
) {
}
