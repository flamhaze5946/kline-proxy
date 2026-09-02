package com.zx.quant.klineproxy.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class HourBoundaryGuardTest {

  private static final long BEFORE = 150_000L;
  private static final long AFTER = 30_000L;
  private static final long HOUR = HourBoundaryGuard.HOUR_MS;
  // an arbitrary hour boundary (2026-09-02T13:00:00Z)
  private static final long T = 1_788_354_000_000L;

  @Test
  void skipsInsideTheWindowBeforeTheBoundary() {
    assertTrue(HourBoundaryGuard.shouldSkip(T - 150_000L, BEFORE, AFTER)); // :57:30 exactly
    assertTrue(HourBoundaryGuard.shouldSkip(T - 120_000L, BEFORE, AFTER)); // :58:00
    assertTrue(HourBoundaryGuard.shouldSkip(T - 1L, BEFORE, AFTER));       // :59:59.999
    assertTrue(HourBoundaryGuard.shouldSkip(T, BEFORE, AFTER));            // :00:00.000
  }

  @Test
  void skipsInsideTheWindowAfterTheBoundary() {
    assertTrue(HourBoundaryGuard.shouldSkip(T + 10_000L, BEFORE, AFTER));  // :00:10
    assertTrue(HourBoundaryGuard.shouldSkip(T + 29_999L, BEFORE, AFTER));  // :00:29.999
    assertFalse(HourBoundaryGuard.shouldSkip(T + 30_000L, BEFORE, AFTER)); // :00:30 runs
  }

  @Test
  void runsOutsideTheWindow() {
    assertFalse(HourBoundaryGuard.shouldSkip(T - 150_001L, BEFORE, AFTER)); // :57:29.999
    assertFalse(HourBoundaryGuard.shouldSkip(T - 180_000L, BEFORE, AFTER)); // :57:00
    assertFalse(HourBoundaryGuard.shouldSkip(T + 30L * 60L * 1000L, BEFORE, AFTER)); // :30:00
    assertFalse(HourBoundaryGuard.shouldSkip(T + HOUR - 150_001L, BEFORE, AFTER));
  }

  @Test
  void zeroOrNegativeGuardsDisableThatSide() {
    assertFalse(HourBoundaryGuard.shouldSkip(T - 1L, 0L, AFTER));
    assertFalse(HourBoundaryGuard.shouldSkip(T + 1L, BEFORE, 0L));
    assertFalse(HourBoundaryGuard.shouldSkip(T - 1L, -1L, -1L));
    assertFalse(HourBoundaryGuard.shouldSkip(T + 1L, -1L, -1L));
  }

  @Test
  void windowIsShorterThanTheSyncPeriodSoOneSkipAlwaysSuffices() {
    long period = 5L * 60L * 1000L;
    assertTrue(BEFORE + AFTER < period);
    // a tick skipped at :58:00 lands at :03:00 — outside the window
    assertFalse(HourBoundaryGuard.shouldSkip(T - 120_000L + period, BEFORE, AFTER));
  }
}
