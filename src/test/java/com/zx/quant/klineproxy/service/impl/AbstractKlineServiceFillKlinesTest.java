package com.zx.quant.klineproxy.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Kline.StringKline;
import com.zx.quant.klineproxy.model.KlineSet;
import com.zx.quant.klineproxy.model.KlineSetKey;
import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.model.Ticker.BigDecimalTicker;
import com.zx.quant.klineproxy.model.Ticker24Hr;
import com.zx.quant.klineproxy.model.config.KlinePersistenceProperties;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties.IntervalSyncConfig;
import com.zx.quant.klineproxy.model.enums.IntervalEnum;
import com.zx.quant.klineproxy.model.persistence.PersistedKlineRow;
import com.zx.quant.klineproxy.service.KlinePersistenceStore;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import com.zx.quant.klineproxy.model.BulkKlinesResponse;
import com.zx.quant.klineproxy.model.config.KlineBulkProperties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

class AbstractKlineServiceFillKlinesTest {

  @Test
  void updateKlinesShouldFillMissingKlineWithPreviousCloseAndBinanceStyleCloseTime() {
    TestKlineService service = new TestKlineService();
    StringKline first = new StringKline();
    first.setOpenTime(0);
    first.setCloseTime(IntervalEnum.ONE_HOUR.getMills() - 1);
    first.setOpenPrice("90");
    first.setHighPrice("110");
    first.setLowPrice("80");
    first.setClosePrice("100");
    first.setTradeNum(10);

    StringKline third = new StringKline();
    third.setOpenTime(IntervalEnum.ONE_HOUR.getMills() * 2);
    third.setCloseTime((IntervalEnum.ONE_HOUR.getMills() * 3) - 1);
    third.setOpenPrice("120");
    third.setHighPrice("130");
    third.setLowPrice("110");
    third.setClosePrice("125");
    third.setTradeNum(20);

    service.updateKlines("BTCUSDT", IntervalEnum.ONE_HOUR.code(), List.of(first, third));

    KlineSet klineSet = service.klineSetMap.get(new KlineSetKey("BTCUSDT", IntervalEnum.ONE_HOUR.code()));
    assertNotNull(klineSet);
    StringKline filled = (StringKline) klineSet.getKlineMap().get(IntervalEnum.ONE_HOUR.getMills());
    assertNotNull(filled);
    assertEquals("100", filled.getOpenPrice());
    assertEquals("100", filled.getHighPrice());
    assertEquals("100", filled.getLowPrice());
    assertEquals("100", filled.getClosePrice());
    assertEquals(0, filled.getTradeNum());
    assertEquals((IntervalEnum.ONE_HOUR.getMills() * 2) - 1, filled.getCloseTime());
  }

  @Test
  void queryBulkKlinesShouldReturnLastClosedRowsAndOmitUnsubscribedSymbols() {
    TestKlineService service = new TestKlineService();
    long hour = IntervalEnum.ONE_HOUR.getMills();
    service.updateKlines("BTCUSDT", IntervalEnum.ONE_HOUR.code(),
        List.of(buildKline(0, "100"), buildKline(hour, "101"), buildKline(hour * 2, "102"),
            buildKline(hour * 3, "103")));
    service.setServerTime((hour * 3) - 1);

    var response = service.queryBulkKlines(IntervalEnum.ONE_HOUR.code(), 2, true,
        List.of("MISSING", "BTCUSDT"));

    assertEquals(List.of("BTCUSDT"), new ArrayList<>(response.klines().keySet()));
    assertEquals(hour, response.klines().get("BTCUSDT").get(0)[0]);
    assertEquals(hour * 2, response.klines().get("BTCUSDT").get(1)[0]);
    assertEquals("101", response.klines().get("BTCUSDT").get(0)[4]);
    assertEquals("102", response.klines().get("BTCUSDT").get(1)[4]);
  }

  @Test
  void updateKlinesShouldTrimSeriesWhenMaintainCountExceeded() {
    TestKlineService service = new TestKlineService();
    List<Kline> klines = IntStream.range(0, 60)
        .mapToObj(index -> buildKline(index * IntervalEnum.ONE_HOUR.getMills(), String.valueOf(index)))
        .map(Kline.class::cast)
        .toList();

    service.updateKlines("BTCUSDT", IntervalEnum.ONE_HOUR.code(), klines);

    KlineSet klineSet = service.klineSetMap.get(new KlineSetKey("BTCUSDT", IntervalEnum.ONE_HOUR.code()));
    assertNotNull(klineSet);
    assertEquals(3, klineSet.getKlineMap().size());
    assertEquals(57L * IntervalEnum.ONE_HOUR.getMills(), klineSet.getKlineMap().firstKey());
    assertEquals(59L * IntervalEnum.ONE_HOUR.getMills(), klineSet.getKlineMap().lastKey());
  }

  @Test
  void buildExpectedTopicsShouldReuseSnapshotUntilInputsChange() {
    TestKlineService service = new TestKlineService();
    service.setSymbols(List.of("BTCUSDT", "ETHBTC"));

    Set<String> firstTopics = service.currentExpectedTopics();
    Set<String> secondTopics = service.currentExpectedTopics();

    assertSame(firstTopics, secondTopics);
    assertEquals(Set.of("!ticker@arr", "btcusdt@kline_1h"), firstTopics);

    service.setSymbols(List.of("BTCUSDT", "ETHUSDT"));
    Set<String> thirdTopics = service.currentExpectedTopics();

    assertNotSame(firstTopics, thirdTopics);
    assertEquals(Set.of("!ticker@arr", "btcusdt@kline_1h", "ethusdt@kline_1h"), thirdTopics);
  }

  @Test
  void queryTickersShouldReuseShortLivedAllMarketSnapshot() {
    TestKlineService service = new TestKlineService();
    service.setRealtimeTickers(List.of(buildTicker("BTCUSDT", "100")));

    List<Ticker<?>> firstTickers = service.queryTickers(List.of());
    List<Ticker<?>> secondTickers = service.queryTickers(List.of());

    assertSame(firstTickers, secondTickers);
    assertEquals(1, service.getRealtimeTickerQueryCount());
  }

  @Test
  void queryTicker24hrsShouldReuseShortLivedAllMarketSnapshot() {
    TestKlineService service = new TestKlineService();
    service.setRealtimeTicker24Hrs(List.of(buildTicker24Hr("BTCUSDT", "100")));

    List<Ticker24Hr> firstTicker24Hrs = service.queryTicker24hrs(List.of());
    List<Ticker24Hr> secondTicker24Hrs = service.queryTicker24hrs(List.of());

    assertSame(firstTicker24Hrs, secondTicker24Hrs);
    assertEquals(1, service.getRealtimeTicker24HrQueryCount());
  }

  @Test
  void staleAllMarketTickerSnapshotShouldReturnCurrentSnapshotWithoutBlockingRefresh() throws Exception {
    TestKlineService service = new TestKlineService();
    service.setRealtimeTickers(List.of(buildTicker("BTCUSDT", "100")));
    List<Ticker<?>> firstTickers = service.queryTickers(List.of());
    assertEquals(1, service.getRealtimeTickerQueryCount());

    service.setRealtimeTickers(List.of(buildTicker("BTCUSDT", "101")));
    service.setRealtimeTickerDelayMillis(250L);
    Thread.sleep(550L);

    long startTime = System.nanoTime();
    List<Ticker<?>> staleTickers = service.queryTickers(List.of());
    long durationMillis = (System.nanoTime() - startTime) / 1_000_000L;

    assertSame(firstTickers, staleTickers);
    assertTrue(durationMillis < 200L);
    assertTrue(awaitCondition(() -> service.getRealtimeTickerQueryCount() >= 2));

    service.setRealtimeTickerDelayMillis(0L);
    assertTrue(awaitCondition(() -> "101".equals(service.queryTickers(List.of()).get(0).getPrice().toString())));
  }

  @Test
  void staleAllMarketTicker24HrSnapshotShouldReturnCurrentSnapshotWithoutBlockingRefresh() throws Exception {
    TestKlineService service = new TestKlineService();
    service.setRealtimeTicker24Hrs(List.of(buildTicker24Hr("BTCUSDT", "100")));
    List<Ticker24Hr> firstTicker24Hrs = service.queryTicker24hrs(List.of());
    assertEquals(1, service.getRealtimeTicker24HrQueryCount());

    service.setRealtimeTicker24Hrs(List.of(buildTicker24Hr("BTCUSDT", "101")));
    service.setRealtimeTicker24HrDelayMillis(250L);
    Thread.sleep(550L);

    long startTime = System.nanoTime();
    List<Ticker24Hr> staleTicker24Hrs = service.queryTicker24hrs(List.of());
    long durationMillis = (System.nanoTime() - startTime) / 1_000_000L;

    assertSame(firstTicker24Hrs, staleTicker24Hrs);
    assertTrue(durationMillis < 200L);
    assertTrue(awaitCondition(() -> service.getRealtimeTicker24HrQueryCount() >= 2));

    service.setRealtimeTicker24HrDelayMillis(0L);
    assertTrue(awaitCondition(() -> new BigDecimal("101").equals(service.queryTicker24hrs(List.of()).get(0).getLastPrice())));
  }

  @Test
  void restoredPersistedKlinesShouldWarmUpOnlyMissingRanges() {
    long hour = IntervalEnum.ONE_HOUR.getMills();
    TestKlineService service = new TestKlineService();
    RecordingPersistenceStore persistenceStore = new RecordingPersistenceStore();
    KlineSetKey klineSetKey = new KlineSetKey("BTCUSDT", IntervalEnum.ONE_HOUR.code());
    persistenceStore.setLoadRows("spot", IntervalEnum.ONE_HOUR.code(), "BTCUSDT", List.of(
        buildPersistedRow(hour, "101"),
        buildPersistedRow(hour * 3, "103")));
    service.configurePersistence(persistenceStore, 4, Map.of());
    service.setServerTime((hour * 4) + 5_000L);
    service.putSymbolOnboardTime("BTCUSDT", 0L);
    service.setQueryKlinesResult(new QueryKlineRequest("BTCUSDT", IntervalEnum.ONE_HOUR.code(), hour * 2, hour * 2, 1),
        List.of(buildKline(hour * 2, "102")));
    service.setQueryKlinesResult(new QueryKlineRequest("BTCUSDT", IntervalEnum.ONE_HOUR.code(), hour * 4, hour * 4, 1),
        List.of(buildKline(hour * 4, "104")));

    Set<KlineSetKey> restoredKeys = service.invokeRestorePersistedKlines(Set.of(klineSetKey));
    service.invokeWarmUpPersistedKlines(restoredKeys);

    assertEquals(Set.of(klineSetKey), restoredKeys);
    // restore loads only the serving window (minMaintainCount=3), not the persistence depth
    assertEquals(3, persistenceStore.getLastLoadMaxStoreCount("spot", IntervalEnum.ONE_HOUR.code(), "BTCUSDT"));
    assertEquals(List.of(
            new QueryKlineRequest("BTCUSDT", IntervalEnum.ONE_HOUR.code(), hour * 2, hour * 2, 1),
            new QueryKlineRequest("BTCUSDT", IntervalEnum.ONE_HOUR.code(), hour * 4, hour * 4, 1)),
        service.getQueryRequests());
    KlineSet klineSet = service.klineSetMap.get(klineSetKey);
    assertNotNull(klineSet);
    assertEquals(List.of(hour, hour * 2, hour * 3, hour * 4), new ArrayList<>(klineSet.getKlineMap().keySet()));
  }

  @Test
  void dumpPersistedKlinesShouldExcludeUnclosedLastKlineAndUseDefaultMaxStoreCount() {
    long hour = IntervalEnum.ONE_HOUR.getMills();
    TestKlineService service = new TestKlineService();
    RecordingPersistenceStore persistenceStore = new RecordingPersistenceStore();
    service.configurePersistence(persistenceStore, null, Map.of());
    service.setServerTime(hour + (hour / 2));
    service.updateKlines("BTCUSDT", IntervalEnum.ONE_HOUR.code(),
        List.of(buildKline(0, "100"), buildKline(hour, "101")));

    service.invokeDumpPersistedKlines(true);

    DumpInvocation dumpInvocation = persistenceStore.getLastDumpInvocation("spot", IntervalEnum.ONE_HOUR.code(), "BTCUSDT");
    assertNotNull(dumpInvocation);
    assertEquals(6, dumpInvocation.maxStoreCount());
    assertEquals(1, dumpInvocation.rows().size());
    assertEquals(0L, dumpInvocation.rows().get(0).getOpenTime());
    assertEquals(hour - 1, dumpInvocation.rows().get(0).getCloseTime());
    assertEquals(service.getServerTimeForTest(), dumpInvocation.currentTime());
  }

  @Test
  void restorePersistedKlinesShouldLoadOnlyMinMaintainCount() {
    TestKlineService service = new TestKlineService();
    RecordingPersistenceStore persistenceStore = new RecordingPersistenceStore();
    service.configurePersistence(persistenceStore, 4, Map.of("BTCUSDT", 7));

    service.invokeRestorePersistedKlines(Set.of(new KlineSetKey("BTCUSDT", IntervalEnum.ONE_HOUR.code())));

    // persistence store counts (interval 4 / symbol 7) govern the dump depth only;
    // startup restore reads just the serving window (minMaintainCount=3)
    assertEquals(3, persistenceStore.getLastLoadMaxStoreCount("spot", IntervalEnum.ONE_HOUR.code(), "BTCUSDT"));
  }

  @Test
  void initialBackgroundSyncShouldUsePersistenceMaxStoreCountWhenGreaterThanMinMaintainCount() {
    long hour = IntervalEnum.ONE_HOUR.getMills();
    TestKlineService service = new TestKlineService();
    RecordingPersistenceStore persistenceStore = new RecordingPersistenceStore();
    service.setSymbols(List.of("ETHUSDT"));
    service.configurePersistence(persistenceStore, 5, Map.of());
    // 60 s after the boundary: outside HourBoundaryGuard's after-window (the 1.7.13 value of
    // +5 s made the guard skip the sync and the test fail regardless of the code under test)
    service.setServerTime((hour * 10) + 60_000L);
    service.putSymbolOnboardTime("ETHUSDT", 0L);

    service.invokeSyncConfiguredKlinesOnce();

    assertEquals(5, service.getQueryRequests().size());
  }

  private static boolean awaitCondition(CheckedBooleanSupplier supplier) throws Exception {
    long deadline = System.currentTimeMillis() + 3_000L;
    while (System.currentTimeMillis() < deadline) {
      if (supplier.getAsBoolean()) {
        return true;
      }
      Thread.sleep(20L);
    }
    return supplier.getAsBoolean();
  }

  @FunctionalInterface
  private interface CheckedBooleanSupplier {
    boolean getAsBoolean() throws Exception;
  }

  private static StringKline buildKline(long openTime, String closePrice) {
    StringKline kline = new StringKline();
    kline.setOpenTime(openTime);
    kline.setCloseTime(openTime + IntervalEnum.ONE_HOUR.getMills() - 1);
    kline.setOpenPrice(closePrice);
    kline.setHighPrice(closePrice);
    kline.setLowPrice(closePrice);
    kline.setClosePrice(closePrice);
    kline.setTradeNum(1);
    return kline;
  }

  private static Ticker<?> buildTicker(String symbol, String price) {
    BigDecimalTicker ticker = new BigDecimalTicker();
    ticker.setSymbol(symbol);
    ticker.setPrice(new BigDecimal(price));
    ticker.setTime(1L);
    return ticker;
  }

  private static Ticker24Hr buildTicker24Hr(String symbol, String lastPrice) {
    Ticker24Hr ticker24Hr = new Ticker24Hr();
    ticker24Hr.setSymbol(symbol);
    ticker24Hr.setLastPrice(new BigDecimal(lastPrice));
    ticker24Hr.setCloseTime(1L);
    return ticker24Hr;
  }

  private static PersistedKlineRow buildPersistedRow(long openTime, String closePrice) {
    PersistedKlineRow row = new PersistedKlineRow();
    row.setOpenTime(openTime);
    row.setOpenPrice(closePrice);
    row.setHighPrice(closePrice);
    row.setLowPrice(closePrice);
    row.setClosePrice(closePrice);
    row.setVolume("1");
    row.setCloseTime(openTime + IntervalEnum.ONE_HOUR.getMills() - 1);
    row.setQuoteVolume("1");
    row.setTradeNum(1);
    row.setActiveBuyVolume("1");
    row.setActiveBuyQuoteVolume("1");
    return row;
  }

  private record QueryKlineRequest(String symbol, String interval, Long startTime, Long endTime, Integer limit) {
  }

  private record StoreKey(String service, String interval, String symbol) {
  }

  private record DumpInvocation(List<PersistedKlineRow> rows, int maxStoreCount, long currentTime) {
  }

  private static class RecordingPersistenceStore implements KlinePersistenceStore {

    private final Map<StoreKey, List<PersistedKlineRow>> loadRowsByKey = new HashMap<>();

    private final Map<StoreKey, Integer> lastLoadMaxStoreCount = new HashMap<>();

    private final Map<StoreKey, DumpInvocation> lastDumpInvocations = new HashMap<>();

    private void setLoadRows(String service, String interval, String symbol, List<PersistedKlineRow> rows) {
      loadRowsByKey.put(new StoreKey(service, interval, symbol), List.copyOf(rows));
    }

    private int getLastLoadMaxStoreCount(String service, String interval, String symbol) {
      return lastLoadMaxStoreCount.getOrDefault(new StoreKey(service, interval, symbol), 0);
    }

    private DumpInvocation getLastDumpInvocation(String service, String interval, String symbol) {
      return lastDumpInvocations.get(new StoreKey(service, interval, symbol));
    }

    @Override
    public List<PersistedKlineRow> loadRows(String service, String interval, String symbol, int maxStoreCount) {
      StoreKey key = new StoreKey(service, interval, symbol);
      lastLoadMaxStoreCount.put(key, maxStoreCount);
      return loadRowsByKey.getOrDefault(key, List.of());
    }

    @Override
    public void dumpRows(String service, String interval, String symbol, List<PersistedKlineRow> rows,
                         int maxStoreCount, long currentTime) {
      lastDumpInvocations.put(new StoreKey(service, interval, symbol),
          new DumpInvocation(List.copyOf(rows), maxStoreCount, currentTime));
    }
  }


  // ---- 1.7.14 bulk closed-bar finality --------------------------------------------------------

  private static final long H = IntervalEnum.ONE_HOUR.getMills();

  private static StringKline hourBar(long openTime, int tradeNum, String close) {
    StringKline bar = new StringKline();
    bar.setOpenTime(openTime);
    bar.setCloseTime(openTime + H - 1);
    bar.setOpenPrice("100");
    bar.setHighPrice("110");
    bar.setLowPrice("90");
    bar.setClosePrice(close);
    bar.setTradeNum(tradeNum);
    return bar;
  }

  private static KlineBulkProperties bulkProps(long maxWaitMs) {
    KlineBulkProperties props = new KlineBulkProperties();
    props.setFinalWaitEnabled(true);
    props.setFinalWaitMaxMs(maxWaitMs);
    return props;
  }

  private static TestKlineService finalWaitService(long serverTime, long maxWaitMs) {
    TestKlineService service = new TestKlineService();
    service.serverTime = serverTime;
    ReflectionTestUtils.setField(service, "bulkProperties", bulkProps(maxWaitMs));
    return service;
  }

  @Test
  void streamBarIsFinalOnlyWhenBinanceSaysClosed() {
    long boundary = 10 * H;
    // 100 ms after the boundary with a 300 ms cap: the request must wait ~200 ms then give up
    TestKlineService service = finalWaitService(boundary + 100, 300);
    // pre-close snapshot of the just-closed bar (x=false) and the forming bar
    service.updateStreamKline("BTCUSDT", "1h", hourBar(boundary - H, 5, "100"), false);
    service.updateStreamKline("BTCUSDT", "1h", hourBar(boundary, 1, "101"), false);
    BulkKlinesResponse first = service.queryBulkKlines("1h", 2, true, List.of("BTCUSDT"));
    assertThat(first.finalized()).isFalse();
    assertThat(first.pending()).containsExactly("BTCUSDT");
    assertThat(first.waitedMs()).isGreaterThanOrEqualTo(150);
    // closing update (x=true) — same trade count is fine: the flag is what matters
    service.updateStreamKline("BTCUSDT", "1h", hourBar(boundary - H, 5, "100"), true);
    BulkKlinesResponse second = service.queryBulkKlines("1h", 2, true, List.of("BTCUSDT"));
    assertThat(second.finalized()).isTrue();
    assertThat(second.pending()).isEmpty();
    assertThat(second.waitedMs()).isZero();
    // the non-final response was not cached: the second answer differs
    assertThat(second).isNotSameAs(first);
  }

  @Test
  void bulkBlocksUntilTheClosingUpdateArrives() throws Exception {
    long boundary = 10 * H;
    TestKlineService service = finalWaitService(boundary + 500, 5_000);
    service.updateStreamKline("ETHUSDT", "1h", hourBar(boundary - H, 7, "200"), false);
    service.updateStreamKline("ADAUSDT", "1h", hourBar(boundary - H, 3, "1"), true);
    CountDownLatch released = new CountDownLatch(1);
    Thread closer = new Thread(() -> {
      try {
        Thread.sleep(150);
      } catch (InterruptedException ignored) {
        return;
      }
      service.updateStreamKline("ETHUSDT", "1h", hourBar(boundary - H, 7, "200"), true);
      released.countDown();
    });
    closer.start();
    long started = System.nanoTime();
    BulkKlinesResponse response = service.queryBulkKlines("1h", 1, true, List.of("ETHUSDT", "ADAUSDT"));
    long elapsedMs = (System.nanoTime() - started) / 1_000_000L;
    assertThat(released.await(1, TimeUnit.SECONDS)).isTrue();
    assertThat(response.finalized()).isTrue();
    assertThat(response.pending()).isEmpty();
    assertThat(response.waitedMs()).isBetween(100L, 4_000L);
    assertThat(elapsedMs).isLessThan(4_000L);
    assertThat(response.klines()).containsKeys("ETHUSDT", "ADAUSDT");
  }

  @Test
  void symbolsWithoutTheJustClosedBarAreNotWaitedForAndRestBarsAreFinal() {
    long boundary = 10 * H;
    TestKlineService service = finalWaitService(boundary + 1_000, 3_000);
    // REST-synced closed bar: final; forming bar from REST: not final (irrelevant for closed_only)
    service.updateKlines("BNBUSDT", "1h", List.of(hourBar(boundary - H, 9, "300"), hourBar(boundary, 1, "301")));
    // XRP only has an older bar: nothing to wait for
    service.updateStreamKline("XRPUSDT", "1h", hourBar(boundary - 2 * H, 4, "0.5"), false);
    long started = System.nanoTime();
    BulkKlinesResponse response = service.queryBulkKlines("1h", 3, true, List.of("BNBUSDT", "XRPUSDT"));
    assertThat((System.nanoTime() - started) / 1_000_000L).isLessThan(500L);
    assertThat(response.finalized()).isTrue();
    assertThat(response.waitedMs()).isZero();
  }

  @Test
  void outsideTheSettlingWindowAndWithClosedOnlyFalseThereIsNoWait() {
    long boundary = 10 * H;
    TestKlineService service = finalWaitService(boundary + 9_000, 3_000);
    service.updateStreamKline("SOLUSDT", "1h", hourBar(boundary - H, 2, "20"), false);
    long started = System.nanoTime();
    BulkKlinesResponse late = service.queryBulkKlines("1h", 1, true, List.of("SOLUSDT"));
    assertThat((System.nanoTime() - started) / 1_000_000L).isLessThan(500L);
    assertThat(late.finalized()).isFalse();
    assertThat(late.pending()).containsExactly("SOLUSDT");
    assertThat(late.waitedMs()).isZero();
    service.serverTime = boundary + 1_000;
    started = System.nanoTime();
    BulkKlinesResponse open = service.queryBulkKlines("1h", 1, false, List.of("SOLUSDT"));
    assertThat((System.nanoTime() - started) / 1_000_000L).isLessThan(500L);
    assertThat(open.waitedMs()).isZero();
  }

  @Test
  void cacheKeyIncludesTheBoundary() {
    long boundary = 10 * H;
    TestKlineService service = finalWaitService(boundary + 1_000, 100);
    service.updateStreamKline("DOGEUSDT", "1h", hourBar(boundary - H, 3, "0.1"), true);
    BulkKlinesResponse first = service.queryBulkKlines("1h", 1, true, List.of("DOGEUSDT"));
    assertThat(first.finalized()).isTrue();
    assertThat(service.queryBulkKlines("1h", 1, true, List.of("DOGEUSDT"))).isSameAs(first);
    // next hour: the new just-closed bar is not final yet; the old cached answer must not be served
    service.serverTime = boundary + H + 1_000;
    service.updateStreamKline("DOGEUSDT", "1h", hourBar(boundary, 4, "0.2"), false);
    BulkKlinesResponse next = service.queryBulkKlines("1h", 1, true, List.of("DOGEUSDT"));
    assertThat(next).isNotSameAs(first);
    assertThat(next.finalized()).isFalse();
    assertThat(next.pending()).containsExactly("DOGEUSDT");
  }

  @Test
  void syntheticFillsAreFinal() {
    long boundary = 10 * H;
    TestKlineService service = finalWaitService(boundary + 1_000, 100);
    // gap between two REST bars is filled synthetically; the fill for boundary-H must be final
    service.updateKlines("LTCUSDT", "1h", List.of(hourBar(boundary - 2 * H, 6, "50"), hourBar(boundary, 1, "51")));
    BulkKlinesResponse response = service.queryBulkKlines("1h", 2, true, List.of("LTCUSDT"));
    assertThat(response.finalized()).isTrue();
    assertThat(response.waitedMs()).isZero();
  }

  private static class TestKlineService extends AbstractKlineService<WebSocketClient> {

    private final KlineSyncConfigProperties syncConfig;

    private List<String> symbols = List.of();

    private List<Ticker<?>> realtimeTickers = List.of();

    private List<Ticker24Hr> realtimeTicker24Hrs = List.of();

    private final AtomicInteger realtimeTickerQueryCount = new AtomicInteger(0);

    private final AtomicInteger realtimeTicker24HrQueryCount = new AtomicInteger(0);

    private volatile long realtimeTickerDelayMillis;

    private volatile long realtimeTicker24HrDelayMillis;

    private volatile long serverTime = System.currentTimeMillis();

    private final List<QueryKlineRequest> queryRequests = new ArrayList<>();

    private final Map<QueryKlineRequest, List<Kline>> queryResults = new HashMap<>();

    private TestKlineService() {
      IntervalSyncConfig intervalSyncConfig = new IntervalSyncConfig();
      intervalSyncConfig.setMinMaintainCount(3);
      intervalSyncConfig.setListenSymbolPatterns(List.of(".*USDT"));
      syncConfig = new KlineSyncConfigProperties();
      syncConfig.setIntervalSyncConfigs(Map.of(IntervalEnum.ONE_HOUR.code(), intervalSyncConfig));
      this.rateLimitManager = mock(RateLimitManager.class);
      ReflectionTestUtils.setField(this, "numberType", "string");
    }

    private void setSymbols(List<String> symbols) {
      this.symbols = symbols;
    }

    private Set<String> currentExpectedTopics() {
      return buildExpectedTopics();
    }

    private void setRealtimeTickers(List<Ticker<?>> realtimeTickers) {
      this.realtimeTickers = realtimeTickers;
    }

    private void setRealtimeTicker24Hrs(List<Ticker24Hr> realtimeTicker24Hrs) {
      this.realtimeTicker24Hrs = realtimeTicker24Hrs;
    }

    private int getRealtimeTickerQueryCount() {
      return realtimeTickerQueryCount.get();
    }

    private int getRealtimeTicker24HrQueryCount() {
      return realtimeTicker24HrQueryCount.get();
    }

    private void setRealtimeTickerDelayMillis(long realtimeTickerDelayMillis) {
      this.realtimeTickerDelayMillis = realtimeTickerDelayMillis;
    }

    private void setRealtimeTicker24HrDelayMillis(long realtimeTicker24HrDelayMillis) {
      this.realtimeTicker24HrDelayMillis = realtimeTicker24HrDelayMillis;
    }

    private void setServerTime(long serverTime) {
      this.serverTime = serverTime;
    }

    private long getServerTimeForTest() {
      return serverTime;
    }

    private void configurePersistence(KlinePersistenceStore persistenceStore, Integer intervalMaxStoreCount,
                                      Map<String, Integer> symbolMaxStoreCounts) {
      KlinePersistenceProperties persistenceProperties = new KlinePersistenceProperties();
      persistenceProperties.setEnabled(true);
      KlinePersistenceProperties.IntervalPersistenceConfig intervalPersistenceConfig =
          new KlinePersistenceProperties.IntervalPersistenceConfig();
      intervalPersistenceConfig.setMaxStoreCount(intervalMaxStoreCount);
      intervalPersistenceConfig.setSymbolMaxStoreCounts(new HashMap<>(symbolMaxStoreCounts));
      persistenceProperties.getSpot().getIntervalConfigs().put(IntervalEnum.ONE_HOUR.code(), intervalPersistenceConfig);
      ReflectionTestUtils.setField(this, "persistenceProperties", persistenceProperties);
      ReflectionTestUtils.setField(this, "klinePersistenceStore", persistenceStore);
    }

    @SuppressWarnings("unchecked")
    private void putSymbolOnboardTime(String symbol, long onboardTime) {
      Map<String, Long> symbolOnboardTimeMap =
          (Map<String, Long>) ReflectionTestUtils.getField(this, "symbolOnboardTimeMap");
      assertNotNull(symbolOnboardTimeMap);
      symbolOnboardTimeMap.put(symbol, onboardTime);
    }

    @SuppressWarnings("unchecked")
    private Set<KlineSetKey> invokeRestorePersistedKlines(Set<KlineSetKey> configuredKlineSetKeys) {
      return (Set<KlineSetKey>) ReflectionTestUtils.invokeMethod(this, "restorePersistedKlines", configuredKlineSetKeys);
    }

    private void invokeWarmUpPersistedKlines(Set<KlineSetKey> restoredKlineSetKeys) {
      ReflectionTestUtils.invokeMethod(this, "warmUpPersistedKlines", restoredKlineSetKeys);
    }

    private void invokeDumpPersistedKlines(boolean force) {
      ReflectionTestUtils.invokeMethod(this, "dumpPersistedKlines", force);
    }

    private void invokeSyncConfiguredKlinesOnce() {
      ReflectionTestUtils.invokeMethod(this, "syncConfiguredKlinesOnce");
    }

    private void setQueryKlinesResult(QueryKlineRequest request, List<Kline> klines) {
      queryResults.put(request, List.copyOf(klines));
    }

    private List<QueryKlineRequest> getQueryRequests() {
      return List.copyOf(queryRequests);
    }

    @Override
    protected List<Kline> queryKlines0(String symbol, String interval, Long startTime, Long endTime, Integer limit) {
      QueryKlineRequest request = new QueryKlineRequest(symbol, interval, startTime, endTime, limit);
      queryRequests.add(request);
      return queryResults.getOrDefault(request, List.of());
    }

    @Override
    protected List<Ticker24Hr> queryTicker24Hrs0() {
      realtimeTicker24HrQueryCount.incrementAndGet();
      sleep(realtimeTicker24HrDelayMillis);
      return realtimeTicker24Hrs;
    }

    @Override
    protected List<Ticker<?>> queryTickers0() {
      realtimeTickerQueryCount.incrementAndGet();
      sleep(realtimeTickerDelayMillis);
      return realtimeTickers;
    }

    @Override
    protected String getRateLimiterName() {
      return "test";
    }

    @Override
    protected List<String> getSymbols() {
      return symbols;
    }

    @Override
    protected KlineSyncConfigProperties getSyncConfig() {
      return syncConfig;
    }

    @Override
    protected int getMakeUpKlinesLimit() {
      return 1;
    }

    @Override
    protected int getMakeUpKlinesWeight() {
      return 1;
    }

    @Override
    protected int getTicker24HrsWeight() {
      return 1;
    }

    @Override
    protected String getServiceType() {
      return "test";
    }

    @Override
    protected String getPersistenceServiceCode() {
      return "spot";
    }

    @Override
    protected long getServerTime() {
      return serverTime;
    }

    private void sleep(long delayMillis) {
      if (delayMillis <= 0L) {
        return;
      }
      try {
        Thread.sleep(delayMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }
}
