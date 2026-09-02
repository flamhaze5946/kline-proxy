package com.zx.quant.klineproxy.util;

/**
 * Decides whether a background job must stay away from the hour boundary.
 *
 * <p>Why (2026-09-02): the 5-minute full RPC kline sync (678 futures symbols, ~90 s) is
 * scheduled with a fixed delay, so its phase drifts by ~90 s every hour and about every
 * 4.3 hours it straddles HH:00 — exactly when the trading fleet's 288 shards ask for the
 * just-closed bar. In those hours the closed bars became available 0.2–1.1 s later
 * (nginx {@code rt=} on {@code klines/bulk} at :00, 60-hour contingency 13/1/2/43).
 * The sync is gap-filling only (WebSocket keeps the live candle current), so skipping
 * one tick near the boundary costs nothing; the next tick is 5 minutes later, outside
 * the window by construction because the window ({@code before + after}) is shorter
 * than the period.
 */
public final class HourBoundaryGuard {

  public static final long HOUR_MS = 60L * 60L * 1000L;

  private HourBoundaryGuard() {
  }

  /**
   * @param nowMs        wall-clock (exchange server time) in ms
   * @param beforeMs     how long before the boundary the job must not START (the job's
   *                     own duration plus margin); {@code <= 0} disables this side
   * @param afterMs      how long after the boundary the job must not start (lets the
   *                     fleet's bar requests drain first); {@code <= 0} disables this side
   * @return {@code true} when a job starting now would overlap the protected window
   */
  public static boolean shouldSkip(long nowMs, long beforeMs, long afterMs) {
    long intoHour = Math.floorMod(nowMs, HOUR_MS);
    long toBoundary = HOUR_MS - intoHour;
    if (afterMs > 0 && intoHour < afterMs) {
      return true;
    }
    return beforeMs > 0 && toBoundary <= beforeMs;
  }
}
