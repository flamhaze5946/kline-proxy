package com.zx.quant.klineproxy.model.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Bulk kline endpoint behaviour.
 *
 * <p>{@code finalWaitEnabled}: with {@code closed_only=true}, a request that arrives within
 * {@code finalWaitMaxMs} after an interval boundary blocks until every requested symbol whose
 * just-closed bar exists in memory has received its FINAL update (websocket {@code x=true} or
 * REST), so callers never see a pre-close snapshot as a closed bar. The wait is capped at
 * {@code finalWaitMaxMs} measured from the boundary; symbols without any bar for the just-closed
 * open time are never waited for.
 *
 * <p>{@code preBoundaryWaitMs}: a {@code closed_only=true} request that arrives less than this
 * long BEFORE a boundary first sleeps until the boundary and is then answered as of it, including
 * the final wait above. Such a request is early by its caller's clock or seen early through a
 * clock that trails; answering it from the interval about to close would drop the very bar it
 * asks for. Only active while the final wait is ({@code finalWaitEnabled} and
 * {@code finalWaitMaxMs > 0}); {@code 0} disables it.
 *
 * <p>{@code hostClockBoundary}: the Binance services decide bulk interval boundaries on the host
 * clock, never behind the server-time estimate (which trails the true time by about half a round
 * trip). {@code false} restores the server-time decision used before 2026-09-30.
 */
@Data
@ConfigurationProperties(prefix = "kline.bulk")
public class KlineBulkProperties {

  private static final long MAX_FINAL_WAIT_MS = 30_000L;

  private static final long MAX_PRE_BOUNDARY_WAIT_MS = 2_000L;

  private boolean finalWaitEnabled = true;

  private long finalWaitMaxMs = 8_000L;

  private long preBoundaryWaitMs = 250L;

  private boolean hostClockBoundary = true;

  public long effectiveFinalWaitMaxMs() {
    return Math.max(0L, Math.min(finalWaitMaxMs, MAX_FINAL_WAIT_MS));
  }

  public long effectivePreBoundaryWaitMs() {
    return Math.max(0L, Math.min(preBoundaryWaitMs, MAX_PRE_BOUNDARY_WAIT_MS));
  }
}
