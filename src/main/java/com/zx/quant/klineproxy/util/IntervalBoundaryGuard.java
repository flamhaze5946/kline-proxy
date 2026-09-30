package com.zx.quant.klineproxy.util;

import com.zx.quant.klineproxy.model.enums.IntervalEnum;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;

/** Protects UTC kline boundaries, including calendar weeks and months. */
public final class IntervalBoundaryGuard {

  // Unix epoch is a Thursday; Binance weekly candles start on Monday.
  private static final long MONDAY_OFFSET_MS = 4 * IntervalEnum.ONE_DAY.getMills();

  // Spot and USD-M 3d candles open on epoch day 1 modulo 3 (e.g. 2024-01-01 UTC).
  private static final long THREE_DAY_OFFSET_MS = IntervalEnum.ONE_DAY.getMills();

  private IntervalBoundaryGuard() {
  }

  /** The protected range is [boundary - beforeMs, boundary + afterMs). */
  public static boolean shouldSkip(long nowMs, Collection<IntervalEnum> intervals,
                                   long beforeMs, long afterMs) {
    if (beforeMs <= 0 && afterMs <= 0) {
      return false;
    }
    for (IntervalEnum interval : intervals) {
      long elapsedMs;
      long remainingMs;
      if (interval == IntervalEnum.ONE_MONTH) {
        ZonedDateTime monthStart = Instant.ofEpochMilli(nowMs).atZone(ZoneOffset.UTC)
            .withDayOfMonth(1).toLocalDate().atStartOfDay(ZoneOffset.UTC);
        elapsedMs = nowMs - monthStart.toInstant().toEpochMilli();
        remainingMs = monthStart.plusMonths(1).toInstant().toEpochMilli() - nowMs;
      } else {
        long offsetMs = switch (interval) {
          case ONE_WEEK -> MONDAY_OFFSET_MS;
          case THREE_DAY -> THREE_DAY_OFFSET_MS;
          default -> 0;
        };
        elapsedMs = Math.floorMod(nowMs - offsetMs, interval.getMills());
        remainingMs = interval.getMills() - elapsedMs;
      }
      if (afterMs > 0 && elapsedMs < afterMs || beforeMs > 0 && remainingMs <= beforeMs) {
        return true;
      }
    }
    return false;
  }
}
