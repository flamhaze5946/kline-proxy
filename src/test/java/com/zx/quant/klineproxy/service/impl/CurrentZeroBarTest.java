package com.zx.quant.klineproxy.service.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.zx.quant.klineproxy.controller.BinanceFutureController;
import com.zx.quant.klineproxy.controller.BinanceSpotController;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.KlineSet;
import com.zx.quant.klineproxy.model.KlineSetKey;
import com.zx.quant.klineproxy.model.KlineUpdateSource;
import com.zx.quant.klineproxy.model.enums.IntervalEnum;
import com.zx.quant.klineproxy.util.ConvertUtil;
import com.zx.quant.klineproxy.util.KlineFill;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

class CurrentZeroBarTest {
  private static final long H = 3_600_000;
  private static final long B = Instant.parse("2026-09-28T18:00:00Z").toEpochMilli();

  private ColdDataLoadIntegrationTest.HarnessKlineService service(String mode) {
    var service = new ColdDataLoadIntegrationTest.HarnessKlineService(mode, null);
    service.setServerTime(B + 30_000);
    return service;
  }

  private KlineSet series(ColdDataLoadIntegrationTest.HarnessKlineService service) {
    return service.klineSetMap.get(new KlineSetKey("BTCUSDT", "1h"));
  }

  private void assertZero(Object[] row, long open, long close, Object price) {
    assertEquals(open, row[0]);
    assertEquals(close, row[6]);
    for (int i : new int[]{1, 2, 3, 4}) {
      assertEquals(price, row[i]);
    }
    for (int i : new int[]{5, 7, 8, 9, 10}) {
      assertEquals(0, new BigDecimal(row[i].toString()).signum(), "column " + i);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"string", "float", "double", "bigDecimal"})
  void spotFutureAndBulkExposeOnlyTemporaryCurrentBarAndReplaceItImmediately(String mode) {
    var service = service(mode);
    Kline previous = service.buildServerKline(B - H, "0.0000123400", 12);
    service.updateStreamKline("BTCUSDT", "1h", previous, true);
    Object close = ConvertUtil.convertToDisplayKline(previous)[4];
    var spot = new BinanceSpotController();
    var future = new BinanceFutureController();
    ReflectionTestUtils.setField(spot, "klineService", service);
    ReflectionTestUtils.setField(future, "klineService", service);
    Object[][] spotRows = spot.queryKlines("BTCUSDT", "1h", null, null, 1, null);
    Object[][] futureRows = future.queryKlines("BTCUSDT", "1h", null, null, 1);
    assertEquals(1, spotRows.length);
    assertEquals(1, futureRows.length);
    assertArrayEquals(spotRows[0], futureRows[0]);
    assertZero(spotRows[0], B, B + H - 1, close);
    var bulk = service.queryBulkKlines("1h", 1, false, List.of("BTCUSDT"));
    assertZero(bulk.klines().get("BTCUSDT").getFirst(), B, B + H - 1, close);
    assertSame(bulk, service.queryBulkKlines("1h", 1, false, List.of("BTCUSDT")));
    assertEquals(B - H, service.queryBulkKlines("1h", 1, true, List.of("BTCUSDT"))
        .klines().get("BTCUSDT").getFirst()[0]);
    assertEquals(1, series(service).getKlineMap().size());
    assertFalse(series(service).isFinal(B));
    assertEquals(List.of(previous), series(service).finalSnapshot(B + H + 1));
    assertTrue(service.getQueryRequests().isEmpty());

    // A true zero-trade REST row must win too: placeholder is not a stored competing revision.
    Kline realZero = service.buildServerKline(B, "0.00001235", 0);
    service.updateKlines("BTCUSDT", "1h", List.of(realZero));
    var replaced = service.queryBulkKlines("1h", 1, false, List.of("BTCUSDT"));
    assertNotSame(bulk, replaced);
    assertArrayEquals(ConvertUtil.convertToDisplayKline(realZero), replaced.klines().get("BTCUSDT").getFirst());
    Kline traded = service.buildServerKline(B, "0.00001236", 1);
    service.updateStreamKline("BTCUSDT", "1h", traded, false);
    assertArrayEquals(ConvertUtil.convertToDisplayKline(traded),
        future.queryKlines("BTCUSDT", "1h", null, null, 1)[0]);
    assertFalse(series(service).isFinal(B));
  }

  @Test
  void finalityArrivalAndEmptySeriesInvalidateCachedBulkWithoutWaitingForTtl() {
    var service = service("string");
    var empty = service.queryBulkKlines("1h", 1, false, List.of("BTCUSDT"));
    assertTrue(empty.klines().isEmpty());
    Kline last = service.buildServerKline(B - H, "123", 1);
    service.updateStreamKline("BTCUSDT", "1h", last, false);
    var forming = service.queryBulkKlines("1h", 1, false, List.of("BTCUSDT"));
    assertEquals(B - H, forming.klines().get("BTCUSDT").getFirst()[0]);
    service.updateStreamKline("BTCUSDT", "1h", last, true);
    assertEquals(B, service.queryBulkKlines("1h", 1, false, List.of("BTCUSDT"))
        .klines().get("BTCUSDT").getFirst()[0]);
  }

  @Test
  void currentQueriesKeepLimitAndHistoricalRangesButNeverChainAcrossMissingPeriods() {
    var service = service("double");
    for (int i = 12; i >= 1; i--) {
      service.updateStreamKline("BTCUSDT", "1h", service.buildServerKline(B - i * H, "100", i), true);
    }
    var latest = service.queryKlineList("BTCUSDT", "1h", null, null, 10);
    assertEquals(10, latest.size());
    assertEquals(B - 9 * H, latest.getFirst().getOpenTime());
    assertEquals(B, latest.getLast().getOpenTime());
    assertEquals(B, service.queryKlineList("BTCUSDT", "1h", B, B, 1).getFirst().getOpenTime());
    assertEquals(B - H, service.queryKlineList("BTCUSDT", "1h", null, B - 1, 1).getFirst().getOpenTime());
    assertTrue(service.queryKlineList("BTCUSDT", "1h", B + H, B + H, 1).isEmpty());
    service.setServerTime(B + H);
    assertEquals(B - H, service.queryKlineList("BTCUSDT", "1h", null, null, 1).getFirst().getOpenTime());
  }

  @Test
  void inactiveSymbolsDoNotGainAProvisionalBar() {
    var service = service("double");
    service.buildExpectedTopics();
    service.updateStreamKline("DELISTEDUSDT", "1h", service.buildServerKline(B - H, "1", 1), true);
    assertEquals(B - H, service.queryKlineList("DELISTEDUSDT", "1h", null, null, 1).getFirst().getOpenTime());
  }

  @Test
  void placeholderUsesCalendarMonthAndActualWeeklyBoundary() {
    var service = service("string");
    long feb = Instant.parse("2024-02-01T00:00:00Z").toEpochMilli();
    long march = Instant.parse("2024-03-01T00:00:00Z").toEpochMilli();
    Kline jan = service.buildServerKline(feb - 31L * 86_400_000, "123.4500", 1);
    jan.setCloseTime(feb - 1);
    Kline zero = KlineFill.currentAfter(jan, true, IntervalEnum.ONE_MONTH, march - 1);
    assertNotNull(zero);
    assertZero(ConvertUtil.convertToDisplayKline(zero), feb, march - 1, "123.4500");
    assertNull(KlineFill.currentAfter(jan, true, IntervalEnum.ONE_MONTH, march));
    assertNull(KlineFill.currentAfter(jan, false, IntervalEnum.ONE_MONTH, feb));
    for (IntervalEnum interval : IntervalEnum.values()) {
      if (interval == IntervalEnum.ONE_MONTH) continue;
      Kline previous = service.buildServerKline(B - interval.getMills(), "2", 1);
      previous.setCloseTime(B - 1);
      assertNull(KlineFill.currentAfter(previous, true, interval, B - 1));
      assertEquals(B, KlineFill.currentAfter(previous, true, interval, B).getOpenTime());
      assertEquals(B + interval.getMills() - 1,
          KlineFill.currentAfter(previous, true, interval, B).getCloseTime());
      assertNull(KlineFill.currentAfter(previous, true, interval, B + interval.getMills()));
    }
  }

  @Test
  void monthlyBulkExpiresAtCalendarRolloverEvenInsideLegacyCacheBoundary() {
    var service = service("double");
    long feb = Instant.parse("2024-02-01T00:00:00Z").toEpochMilli();
    long march = Instant.parse("2024-03-01T00:00:00Z").toEpochMilli();
    service.setServerTime(march - 1);
    Kline jan = service.buildServerKline(feb - 31L * 86_400_000, "100", 1);
    jan.setCloseTime(feb - 1);
    service.updateStreamKline("BTCUSDT", "1M", jan, true);
    assertEquals(feb, service.queryKlineList("BTCUSDT", "1M", null, null, 1).getFirst().getOpenTime());
    var old = service.queryBulkKlines("1M", 1, false, List.of("BTCUSDT"));
    assertEquals(feb, old.klines().get("BTCUSDT").getFirst()[0]);
    service.setServerTime(march);
    var next = service.queryBulkKlines("1M", 1, false, List.of("BTCUSDT"));
    assertNotSame(old, next);
    assertEquals(jan.getOpenTime(), next.klines().get("BTCUSDT").getFirst()[0]);
  }

  @Test
  void restRestoreAndSyntheticFinalsRequireThePreviousBarsOwnWebSocketClosingEvent() {
    for (KlineUpdateSource source : List.of(KlineUpdateSource.REST,
        KlineUpdateSource.RESTORE, KlineUpdateSource.SYNTHETIC)) {
      var service = service("string");
      Kline previous = service.buildServerKline(B - H, "100", 1);
      var key = new KlineSetKey("BTCUSDT", "1h");
      var set = new KlineSet(key);
      service.klineSetMap.put(key, set);
      set.commit(previous, true, source, null, 0);
      var before = service.queryBulkKlines("1h", 1, false, List.of("BTCUSDT"));
      assertEquals(B - H, before.klines().get("BTCUSDT").getFirst()[0]);
      service.updateStreamKline("BTCUSDT", "1h", previous, false);
      service.updateStreamKline("BTCUSDT", "1h", service.buildServerKline(B - 2 * H, "99", 1), true);
      assertEquals(B - H, service.queryKlineList("BTCUSDT", "1h", null, null, 1).getFirst().getOpenTime());
      var beforeConfirmation = service.queryBulkKlines("1h", 1, false, List.of("BTCUSDT"));
      long generation = set.getWindowGeneration();
      service.updateStreamKline("BTCUSDT", "1h", previous, true);
      assertTrue(set.getWindowGeneration() > generation);
      var after = service.queryBulkKlines("1h", 1, false, List.of("BTCUSDT"));
      assertNotSame(beforeConfirmation, after);
      assertEquals(B, after.klines().get("BTCUSDT").getFirst()[0]);
      var restored = new KlineSet(key);
      for (Kline row : set.finalSnapshot(B + H)) {
        restored.commit(row, true, KlineUpdateSource.RESTORE, null, 0);
      }
      assertNull(restored.currentPlaceholder(IntervalEnum.ONE_HOUR, B + 30_000));
      service.setServerTime(B + H + 30_000);
      Kline next = service.buildServerKline(B, "101", 2);
      service.updateKlines("BTCUSDT", "1h", List.of(next));
      assertEquals(B, service.queryKlineList("BTCUSDT", "1h", null, null, 1).getFirst().getOpenTime());
      service.updateStreamKline("BTCUSDT", "1h", next, true);
      assertEquals(B + H, service.queryKlineList("BTCUSDT", "1h", null, null, 1).getFirst().getOpenTime());
    }
  }
}
