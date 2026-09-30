package com.zx.quant.klineproxy.service.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.KlineSet;
import com.zx.quant.klineproxy.model.KlineSetKey;
import com.zx.quant.klineproxy.model.KlineUpdateSource;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

class AbstractKlineServiceRestPageTest {
  private static final long HOUR = 3_600_000L;
  private static final KlineSetKey KEY = new KlineSetKey("BTCUSDT", "1h");

  @ParameterizedTest
  @ValueSource(strings = {"string", "float", "double", "bigDecimal"})
  void cachedTailRefreshKeepsValuesAndFinalityWithoutRepeatedSyntheticCommits(String numberType) {
    Service actual = new Service(numberType);
    Service legacy = new Service(numberType);
    List<Kline> original = rows(actual, 0, 1000);
    actual.seed(original);
    legacy.seed(original);
    List<Kline> page = IntStream.range(901, 1000)
        .mapToObj(i -> actual.buildServerKline(i * HOUR, "123.45678901", 2000 + i))
        .toList();
    actual.series.calls.set(0);
    actual.series.synthetic.set(0);

    assertEquals(page, actual.fetch(page));
    page.forEach(kline -> legacy.updateKline("BTCUSDT", "1h", kline));
    assertSameCache(legacy, actual);
    assertEquals(99, actual.series.calls.get());
    assertEquals(0, actual.series.synthetic.get());
    assertEquals(1, actual.getQueryRequests().size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"cold", "sparse", "older", "unordered", "duplicate", "newTail", "missingCachedBar", "empty"})
  void pagesOutsideTheFastPathPreserveExistingGapAndRetentionBehavior(String scenario) {
    Service actual = new Service("double");
    Service legacy = new Service("double");
    if (!scenario.equals("cold")) {
      List<Kline> original = rows(actual, 0, 9);
      actual.seed(original);
      legacy.seed(original);
    }
    List<Kline> page = switch (scenario) {
      case "cold" -> rows(actual, 0, 3);
      case "sparse" -> List.of(row(actual, 4), row(actual, 6), row(actual, 8));
      case "older" -> rows(actual, 1, 4);
      case "unordered" -> List.of(row(actual, 8), row(actual, 7), row(actual, 6));
      case "duplicate" -> List.of(row(actual, 6), row(actual, 7), row(actual, 7), row(actual, 8));
      case "newTail" -> rows(actual, 9, 12);
      case "missingCachedBar" -> rows(actual, 6, 9);
      default -> List.of();
    };
    if (scenario.equals("missingCachedBar")) {
      for (Service service : List.of(actual, legacy)) {
        service.series.getKlineMap().remove(7 * HOUR);
        service.series.getFinalOpenTimes().remove(7 * HOUR);
      }
    }

    assertEquals(page.stream().sorted(java.util.Comparator.comparingLong(Kline::getOpenTime)).toList(), actual.fetch(page));
    page.forEach(kline -> legacy.updateKline("BTCUSDT", "1h", kline));
    assertSameCache(legacy, actual);
  }

  @Test
  void newerWebsocketFinalArrivingDuringBatchRefreshCannotBeRegressed() throws Exception {
    Service service = new Service("double");
    service.seed(rows(service, 0, 1000));
    List<Kline> page = rows(service, 901, 1000);
    CountDownLatch restPaused = new CountDownLatch(1);
    CountDownLatch releaseRest = new CountDownLatch(1);
    long targetOpen = 950 * HOUR;
    service.series.beforeRestCommit = kline -> {
      if (kline.getOpenTime() == targetOpen) {
        restPaused.countDown();
        await(releaseRest);
      }
    };
    try (var pool = Executors.newSingleThreadExecutor()) {
      var result = pool.submit(() -> service.fetch(page));
      try {
        assertTrue(restPaused.await(3, TimeUnit.SECONDS));
        Kline corrected = service.buildServerKline(targetOpen, "999.12345678", 10000);
        service.updateStreamKline("BTCUSDT", "1h", corrected, true, 1001 * HOUR);
        releaseRest.countDown();
        result.get(3, TimeUnit.SECONDS);
        assertEquals(corrected, service.series.getKlineMap().get(targetOpen));
        assertTrue(service.series.isFinal(targetOpen));
      } finally {
        releaseRest.countDown();
      }
    }
  }

  private static void assertSameCache(Service expected, Service actual) {
    assertEquals(expected.series.getKlineMap(), actual.series.getKlineMap());
    assertEquals(expected.series.getFinalOpenTimes(), actual.series.getFinalOpenTimes());
  }

  private static Kline row(Service service, int hour) {
    return service.buildServerKline(hour * HOUR, "100.12345678", 100 + hour);
  }

  private static List<Kline> rows(Service service, int from, int until) {
    return IntStream.range(from, until).mapToObj(i -> row(service, i)).toList();
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("REST refresh was not released");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static class Service extends ColdDataLoadIntegrationTest.HarnessKlineService {
    private final CountingSet series = new CountingSet();

    Service(String numberType) {
      super(numberType, null, 1000);
      setServerTime(1000 * HOUR + HOUR / 2);
      klineSetMap.put(KEY, series);
    }

    void seed(List<Kline> rows) {
      updateKlines("BTCUSDT", "1h", rows);
    }

    List<Kline> fetch(List<Kline> page) {
      stubAnyQueryResult(page);
      Executor inline = Runnable::run;
      return ReflectionTestUtils.invokeMethod(this, "fetchAndStoreKlines", "BTCUSDT", "1h",
          List.of(ImmutablePair.of(0L, 1000 * HOUR)), 1, inline);
    }
  }

  private static class CountingSet extends KlineSet {
    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicInteger synthetic = new AtomicInteger();
    private Consumer<Kline> beforeRestCommit = ignored -> {};

    CountingSet() {
      super(KEY);
    }

    @Override
    public Commit commit(Kline incoming, boolean closed, KlineUpdateSource source, Long eventTime, long sequence) {
      if (source == KlineUpdateSource.REST) {
        beforeRestCommit.accept(incoming);
      }
      calls.incrementAndGet();
      if (source == KlineUpdateSource.SYNTHETIC) {
        synthetic.incrementAndGet();
      }
      return super.commit(incoming, closed, source, eventTime, sequence);
    }
  }
}
