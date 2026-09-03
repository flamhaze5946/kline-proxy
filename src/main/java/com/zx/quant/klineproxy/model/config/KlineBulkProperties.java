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
 */
@Data
@ConfigurationProperties(prefix = "kline.bulk")
public class KlineBulkProperties {

  private static final long MAX_FINAL_WAIT_MS = 30_000L;

  private boolean finalWaitEnabled = true;

  private long finalWaitMaxMs = 8_000L;

  public long effectiveFinalWaitMaxMs() {
    return Math.max(0L, Math.min(finalWaitMaxMs, MAX_FINAL_WAIT_MS));
  }
}
