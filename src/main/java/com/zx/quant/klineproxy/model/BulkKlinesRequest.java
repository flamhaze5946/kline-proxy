package com.zx.quant.klineproxy.model;

import java.util.List;

/**
 * Request body for {@code POST /fapi/v1/klines/bulk}.
 *
 * <p>Mirrors the GET endpoint's query parameters, but accepts a JSON
 * body so callers (e.g. nos-rs trader_calculator) can supply long
 * symbol lists without hitting the ~8 KB URL length limit.
 *
 * <p>Null/blank {@code symbols} preserves the existing GET behavior:
 * return klines for every subscribed symbol on the given interval.
 */
public record BulkKlinesRequest(
    String interval,
    Integer limit,
    Boolean closed_only,
    List<String> symbols
) {
}
