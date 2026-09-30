package com.zx.quant.klineproxy.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.zx.quant.klineproxy.model.enums.IntervalEnum;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

class IntervalBoundaryGuardTest {

  private static final long GUARD_MS = 30_000L;

  @ParameterizedTest
  @EnumSource(value = IntervalEnum.class, names = {"ONE_SECOND", "ONE_MINUTE", "THREE_DAY", "ONE_WEEK", "ONE_MONTH"},
      mode = EnumSource.Mode.EXCLUDE)
  void guardsBothSidesOfFixedIntervalBoundaries(IntervalEnum interval) {
    long boundary = interval.getMills() * 100;
    assertWindow(interval, boundary);
    assertFalse(skip(boundary + interval.getMills() / 2, interval));
  }

  @ParameterizedTest
  @CsvSource({
      "THREE_DAY, 2024-01-01T00:00:00Z",
      "THREE_DAY, 2024-02-03T00:00:00Z",
      "ONE_WEEK, 2024-01-01T00:00:00Z",
      "ONE_WEEK, 2024-01-08T00:00:00Z",
      "ONE_MONTH, 2024-02-01T00:00:00Z",
      "ONE_MONTH, 2024-03-01T00:00:00Z",
      "ONE_MONTH, 2025-03-01T00:00:00Z",
      "ONE_MONTH, 2025-01-01T00:00:00Z"
  })
  void guardsActualCalendarAndExchangeBoundaries(IntervalEnum interval, String boundary) {
    assertWindow(interval, Instant.parse(boundary).toEpochMilli());
  }

  @Test
  void doesNotMistakeEpochMultiplesForThreeDayOrWeeklyBoundaries() {
    assertFalse(skip(Instant.parse("2024-01-03T00:00:00Z").toEpochMilli(), IntervalEnum.THREE_DAY));
    assertFalse(skip(Instant.parse("2024-01-04T00:00:00Z").toEpochMilli(), IntervalEnum.ONE_WEEK));
    assertFalse(skip(Instant.parse("2024-02-29T00:00:00Z").toEpochMilli(), IntervalEnum.ONE_MONTH));
  }

  @Test
  void skipsWhenAnyEnabledIntervalIsNearItsBoundary() {
    List<IntervalEnum> intervals = List.of(IntervalEnum.ONE_HOUR, IntervalEnum.FIVE_MINUTE);
    long boundary = Instant.parse("2026-09-13T12:05:00Z").toEpochMilli();
    assertTrue(IntervalBoundaryGuard.shouldSkip(boundary, intervals, GUARD_MS, GUARD_MS));
    assertFalse(IntervalBoundaryGuard.shouldSkip(boundary + GUARD_MS, intervals, GUARD_MS, GUARD_MS));
    assertFalse(IntervalBoundaryGuard.shouldSkip(boundary, List.of(), GUARD_MS, GUARD_MS));
  }

  @ParameterizedTest
  @EnumSource(value = IntervalEnum.class, names = {"ONE_SECOND", "ONE_MINUTE"})
  void overlappingWindowsBlockTheWholeIntervalUntilTheGuardIsReduced(IntervalEnum interval) {
    long duration = interval.getMills();
    for (long offset : new long[] {0, duration / 4, duration / 2, duration - 1}) {
      assertTrue(skip(offset, interval));
    }
    assertFalse(IntervalBoundaryGuard.shouldSkip(duration / 2, List.of(interval), duration / 4, duration / 4));
  }

  @Test
  void eachSideCanBeConfiguredOrDisabled() {
    long boundary = IntervalEnum.ONE_HOUR.getMills();
    List<IntervalEnum> intervals = List.of(IntervalEnum.ONE_HOUR);
    assertTrue(IntervalBoundaryGuard.shouldSkip(boundary - 45_000, intervals, 60_000, 10_000));
    assertFalse(IntervalBoundaryGuard.shouldSkip(boundary + 10_000, intervals, 60_000, 10_000));
    assertFalse(IntervalBoundaryGuard.shouldSkip(boundary - 1, intervals, 0, GUARD_MS));
    assertFalse(IntervalBoundaryGuard.shouldSkip(boundary + 1, intervals, GUARD_MS, 0));
    assertFalse(IntervalBoundaryGuard.shouldSkip(boundary, intervals, 0, 0));
    assertFalse(IntervalBoundaryGuard.shouldSkip(boundary, intervals, -1, -1));
  }

  private void assertWindow(IntervalEnum interval, long boundary) {
    assertFalse(skip(boundary - GUARD_MS - 1, interval));
    assertTrue(skip(boundary - GUARD_MS, interval));
    assertTrue(skip(boundary - 1, interval));
    assertTrue(skip(boundary, interval));
    assertTrue(skip(boundary + GUARD_MS - 1, interval));
    assertFalse(skip(boundary + GUARD_MS, interval));
  }

  private boolean skip(long now, IntervalEnum interval) {
    return IntervalBoundaryGuard.shouldSkip(now, List.of(interval), GUARD_MS, GUARD_MS);
  }
}
