package com.zx.quant.klineproxy.service.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.KlineSet;
import com.zx.quant.klineproxy.model.KlineSetKey;
import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.model.Ticker24Hr;
import com.zx.quant.klineproxy.model.config.KlinePersistenceProperties;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties.IntervalSyncConfig;
import com.zx.quant.klineproxy.model.enums.IntervalEnum;
import com.zx.quant.klineproxy.util.ConvertUtil;
import com.zx.quant.klineproxy.util.Serializer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * End-to-end cold-data-load tests: dump from one service instance through the REAL
 * {@link JsonKlinePersistenceStore}, then restore into a fresh instance (simulated
 * crash-restart) and verify the served klines are identical and the warmup only
 * fetches the downtime gap.
 */
class ColdDataLoadIntegrationTest {

  private static final long HOUR = IntervalEnum.ONE_HOUR.getMills();

  private static final long BASE_TIME = LocalDate.parse("2026-07-27")
      .atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();

  @TempDir
  Path tempDir;

  @ParameterizedTest
  @ValueSource(strings = {"string", "float", "double", "bigDecimal"})
  void restartShouldServeIdenticalKlinesForAllNumberTypes(String numberType) {
    JsonKlinePersistenceStore store = buildStore();
    HarnessKlineService before = new HarnessKlineService(numberType, store);
    // 30 hours spanning a UTC day boundary, realistic price shapes incl. 8-decimals dust
    List<Kline> sourceKlines = new ArrayList<>();
    String[] closes = {"43250.1", "0.00001234", "123.45678901", "0.00000001", "99999.99999999"};
    for (int i = 0; i < 30; i++) {
      sourceKlines.add(before.buildServerKline(BASE_TIME + i * HOUR, closes[i % closes.length], 10 + i));
    }
    before.updateKlines("BTCUSDT", IntervalEnum.ONE_HOUR.code(), sourceKlines);
    before.setServerTime(BASE_TIME + 31 * HOUR);
    before.invokeDumpPersistedKlines(true);
    NavigableMap<Long, Object[]> displayBefore = displayKlines(before, "BTCUSDT");

    HarnessKlineService after = new HarnessKlineService(numberType, store);
    Set<KlineSetKey> restored = after.invokeRestorePersistedKlines(
        Set.of(new KlineSetKey("BTCUSDT", IntervalEnum.ONE_HOUR.code())));

    assertEquals(1, restored.size());
    NavigableMap<Long, Object[]> displayAfter = displayKlines(after, "BTCUSDT");
    assertEquals(displayBefore.keySet(), displayAfter.keySet());
    displayBefore.forEach((openTime, expected) ->
        assertArrayEquals(expected, displayAfter.get(openTime),
            () -> numberType + " kline mismatch at " + Instant.ofEpochMilli(openTime)));
  }

  @Test
  void restoreShouldSkipCorruptShardAndTmpLeftoverFromCrash() throws Exception {
    JsonKlinePersistenceStore store = buildStore();
    HarnessKlineService before = new HarnessKlineService("double", store);
    List<Kline> sourceKlines = new ArrayList<>();
    for (int i = 0; i < 72; i++) {
      sourceKlines.add(before.buildServerKline(BASE_TIME + i * HOUR, "100." + i, 10 + i));
    }
    before.updateKlines("BTCUSDT", IntervalEnum.ONE_HOUR.code(), sourceKlines);
    before.setServerTime(BASE_TIME + 73 * HOUR);
    before.invokeDumpPersistedKlines(true);

    Path symbolDir = tempDir.resolve("spot").resolve("1h").resolve("BTCUSDT");
    // crash artifacts: a truncated shard (middle day) and an orphan tmp file
    Path middleShard = symbolDir.resolve("2026-07-28.json");
    assertTrue(Files.exists(middleShard));
    Files.writeString(middleShard, "{\"service\":\"spot\",\"rows\":[{\"openTi", StandardCharsets.UTF_8);
    Files.writeString(symbolDir.resolve("2026-07-28.json.tmp"), "garbage", StandardCharsets.UTF_8);

    HarnessKlineService after = new HarnessKlineService("double", store);
    Set<KlineSetKey> restored = after.invokeRestorePersistedKlines(
        Set.of(new KlineSetKey("BTCUSDT", IntervalEnum.ONE_HOUR.code())));

    assertEquals(1, restored.size());
    NavigableMap<Long, Object[]> displayAfter = displayKlines(after, "BTCUSDT");
    // day 2026-07-28 (hours 24..47) lost with the corrupt shard; both other days intact
    assertEquals(48, displayAfter.size());
    assertTrue(displayAfter.containsKey(BASE_TIME));
    assertTrue(displayAfter.containsKey(BASE_TIME + 71 * HOUR));
    assertEquals(0, displayAfter.subMap(BASE_TIME + 24 * HOUR, true, BASE_TIME + 47 * HOUR, true).size());
  }

  @Test
  void restoreShouldSkipUnparseableRowAndKeepRemainder() throws Exception {
    JsonKlinePersistenceStore store = buildStore();
    HarnessKlineService before = new HarnessKlineService("double", store);
    List<Kline> sourceKlines = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      sourceKlines.add(before.buildServerKline(BASE_TIME + i * HOUR, "200." + i, 10 + i));
    }
    before.updateKlines("BTCUSDT", IntervalEnum.ONE_HOUR.code(), sourceKlines);
    before.setServerTime(BASE_TIME + 7 * HOUR);
    before.invokeDumpPersistedKlines(true);

    // poison one persisted row in place (unparseable price for the double number type)
    Path shard = tempDir.resolve("spot").resolve("1h").resolve("BTCUSDT").resolve("2026-07-27.json");
    String content = Files.readString(shard, StandardCharsets.UTF_8);
    String poisoned = content.replaceFirst("\"200\\.2\"", "\"not-a-number\"");
    assertTrue(!poisoned.equals(content), "expected to poison one row");
    Files.writeString(shard, poisoned, StandardCharsets.UTF_8);

    HarnessKlineService after = new HarnessKlineService("double", store);
    Set<KlineSetKey> restored = after.invokeRestorePersistedKlines(
        Set.of(new KlineSetKey("BTCUSDT", IntervalEnum.ONE_HOUR.code())));

    // one bad row must not kill startup: the rest of the series restores
    assertEquals(1, restored.size());
    NavigableMap<Long, Object[]> displayAfter = displayKlines(after, "BTCUSDT");
    assertTrue(displayAfter.size() >= 5, "expected the 5 clean rows restored, got " + displayAfter.size());
  }

  @Test
  void restoreShouldIsolateFaultsPerSymbol() throws Exception {
    JsonKlinePersistenceStore store = buildStore();
    HarnessKlineService before = new HarnessKlineService("double", store);
    for (String symbol : List.of("BTCUSDT", "ETHUSDT")) {
      List<Kline> sourceKlines = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        sourceKlines.add(before.buildServerKline(BASE_TIME + i * HOUR, "300." + i, 10 + i));
      }
      before.updateKlines(symbol, IntervalEnum.ONE_HOUR.code(), sourceKlines);
    }
    before.setServerTime(BASE_TIME + 5 * HOUR);
    before.invokeDumpPersistedKlines(true);

    Path btcDir = tempDir.resolve("spot").resolve("1h").resolve("BTCUSDT");
    try {
      // unreadable symbol dir = IO fault on one symbol only
      assertTrue(btcDir.toFile().setReadable(false, false));
      assertTrue(btcDir.toFile().setExecutable(false, false));

      HarnessKlineService after = new HarnessKlineService("double", store);
      Set<KlineSetKey> restored = after.invokeRestorePersistedKlines(Set.of(
          new KlineSetKey("BTCUSDT", IntervalEnum.ONE_HOUR.code()),
          new KlineSetKey("ETHUSDT", IntervalEnum.ONE_HOUR.code())));

      assertEquals(Set.of(new KlineSetKey("ETHUSDT", IntervalEnum.ONE_HOUR.code())), restored);
      assertEquals(4, displayKlines(after, "ETHUSDT").size());
    } finally {
      btcDir.toFile().setExecutable(true, false);
      btcDir.toFile().setReadable(true, false);
    }
  }

  @Test
  void warmupAfterDowntimeShouldFetchOnlyTheGap() {
    JsonKlinePersistenceStore store = buildStore();
    HarnessKlineService before = new HarnessKlineService("double", store);
    List<Kline> sourceKlines = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      sourceKlines.add(before.buildServerKline(BASE_TIME + i * HOUR, "400." + i, 10 + i));
    }
    before.updateKlines("BTCUSDT", IntervalEnum.ONE_HOUR.code(), sourceKlines);
    before.setServerTime(BASE_TIME + 7 * HOUR);
    before.invokeDumpPersistedKlines(true);

    // crash at hour 6 (kline for hour 6 never persisted), restart 2h later
    HarnessKlineService after = new HarnessKlineService("double", store);
    after.setServerTime(BASE_TIME + 8 * HOUR + 5_000L);
    after.putSymbolOnboardTime("BTCUSDT", 0L);
    KlineSetKey key = new KlineSetKey("BTCUSDT", IntervalEnum.ONE_HOUR.code());
    Set<KlineSetKey> restored = after.invokeRestorePersistedKlines(Set.of(key));
    after.invokeWarmUpPersistedKlines(restored);

    // warmup uses fetch-limit-wide pages starting at missing bars (shared makeup semantics):
    // in-page overlap with restored bars is harmless (tradeNum dedup). The recovery
    // properties that matter: the downtime gap is covered, with a bounded request count
    // (vs ~maxStoreCount/limit pages on a cold start without persisted data).
    List<HarnessKlineService.QueryKlineRequest> requests = after.getQueryRequests();
    assertTrue(!requests.isEmpty(), "warmup should fetch the downtime gap");
    assertTrue(requests.size() <= 2, "expected few page requests, got " + requests);
    for (int i = 6; i <= 8; i++) {
      long gapHour = BASE_TIME + i * HOUR;
      assertTrue(requests.stream().anyMatch(request ->
              request.startTime() <= gapHour && gapHour <= request.endTime()),
          () -> "gap hour " + Instant.ofEpochMilli(gapHour) + " not covered by warmup " + requests);
    }
    for (HarnessKlineService.QueryKlineRequest request : requests) {
      assertTrue(request.endTime() <= BASE_TIME + 8 * HOUR,
          () -> "request beyond current open candle: " + request);
    }
  }

  @Test
  void warmupBackfillShouldBeRepersistedToDiskForNextRestart() {
    JsonKlinePersistenceStore store = buildStore();
    // maintain window = the 72h data span, so "no refetch of closed hours" is decidable
    int maintainCount = 72;
    // crash at hour 30: day-2 shard sealed on disk with only hours 24..29
    HarnessKlineService beforeCrash = new HarnessKlineService("double", store, maintainCount);
    List<Kline> initialKlines = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      initialKlines.add(beforeCrash.buildServerKline(BASE_TIME + i * HOUR, "500." + i, 10 + i));
    }
    beforeCrash.updateKlines("BTCUSDT", IntervalEnum.ONE_HOUR.code(), initialKlines);
    beforeCrash.setServerTime(BASE_TIME + 30 * HOUR + 5_000L);
    beforeCrash.invokeDumpPersistedKlines(true);

    // restart after 42h downtime: warmup backfills hours 30..71, reconcile re-persists
    HarnessKlineService rebooted = new HarnessKlineService("double", store, maintainCount);
    rebooted.setServerTime(BASE_TIME + 72 * HOUR + 5_000L);
    rebooted.putSymbolOnboardTime("BTCUSDT", 0L);
    List<Kline> backfillKlines = new ArrayList<>();
    for (int i = 30; i < 72; i++) {
      backfillKlines.add(rebooted.buildServerKline(BASE_TIME + i * HOUR, "500." + i, 10 + i));
    }
    rebooted.stubAnyQueryResult(backfillKlines);
    KlineSetKey key = new KlineSetKey("BTCUSDT", IntervalEnum.ONE_HOUR.code());
    Set<KlineSetKey> restored = rebooted.invokeRestorePersistedKlines(Set.of(key));
    rebooted.invokeWarmUpPersistedKlines(restored);
    rebooted.invokeReconcilePersistedKlines(Set.of(key));

    // a third restart must find ALL 72 closed hours on disk — no Binance fetch needed
    HarnessKlineService thirdBoot = new HarnessKlineService("double", store, maintainCount);
    thirdBoot.setServerTime(BASE_TIME + 72 * HOUR + 5_000L);
    thirdBoot.putSymbolOnboardTime("BTCUSDT", 0L);
    Set<KlineSetKey> thirdRestored = thirdBoot.invokeRestorePersistedKlines(Set.of(key));
    thirdBoot.invokeWarmUpPersistedKlines(thirdRestored);

    assertEquals(1, thirdRestored.size());
    NavigableMap<Long, Object[]> display = displayKlines(thirdBoot, "BTCUSDT");
    assertEquals(72, display.size());
    for (int i = 0; i < 72; i++) {
      assertNotNull(display.get(BASE_TIME + i * HOUR),
          "hour " + i + " missing from disk after warmup backfill");
    }
    // every closed hour came from disk: warmup may only touch the current open candle
    for (HarnessKlineService.QueryKlineRequest request : thirdBoot.getQueryRequests()) {
      assertTrue(request.startTime() >= BASE_TIME + 72 * HOUR,
          () -> "closed hours refetched from Binance on third boot: " + request);
    }
  }

  @Test
  void reconcileMustNotDeleteShardsForUnrestoredKeys() {
    JsonKlinePersistenceStore store = buildStore();
    HarnessKlineService before = new HarnessKlineService("double", store);
    List<Kline> sourceKlines = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      sourceKlines.add(before.buildServerKline(BASE_TIME + i * HOUR, "600." + i, 10 + i));
    }
    before.updateKlines("BTCUSDT", IntervalEnum.ONE_HOUR.code(), sourceKlines);
    before.setServerTime(BASE_TIME + 7 * HOUR);
    before.invokeDumpPersistedKlines(true);

    // restart where the restore FAILED / was skipped: memory is empty for the key,
    // but the post-warmup reconcile over all configured keys must not wipe the disk cache
    HarnessKlineService rebooted = new HarnessKlineService("double", store);
    rebooted.setServerTime(BASE_TIME + 8 * HOUR);
    KlineSetKey key = new KlineSetKey("BTCUSDT", IntervalEnum.ONE_HOUR.code());
    rebooted.invokeReconcilePersistedKlines(Set.of(key));
    rebooted.invokeDumpPersistedKlines(true);

    HarnessKlineService thirdBoot = new HarnessKlineService("double", store);
    Set<KlineSetKey> restored = thirdBoot.invokeRestorePersistedKlines(Set.of(key));
    assertEquals(1, restored.size(), "disk cache was wiped by reconcile over an unrestored key");
    assertEquals(6, displayKlines(thirdBoot, "BTCUSDT").size());
  }

  @Test
  void intervalsNotListedInPersistenceConfigMustNotBePersisted() {
    JsonKlinePersistenceStore store = buildStore();
    // seed disk data through a service whose 1h interval IS opted in
    HarnessKlineService writer = new HarnessKlineService("double", store);
    List<Kline> sourceKlines = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      sourceKlines.add(writer.buildServerKline(BASE_TIME + i * HOUR, "700." + i, 10 + i));
    }
    writer.updateKlines("BTCUSDT", IntervalEnum.ONE_HOUR.code(), sourceKlines);
    writer.setServerTime(BASE_TIME + 5 * HOUR);
    writer.invokeDumpPersistedKlines(true);
    assertTrue(Files.exists(tempDir.resolve("spot").resolve("1h").resolve("BTCUSDT")));

    // a service whose interval is NOT opted in must neither restore nor dump it
    // (day-sharded storage would write one file per day per symbol for e.g. "1d")
    HarnessKlineService optedOut = new HarnessKlineService("double", store);
    KlinePersistenceProperties optedOutProps = (KlinePersistenceProperties)
        ReflectionTestUtils.getField(optedOut, "persistenceProperties");
    assertNotNull(optedOutProps);
    optedOutProps.getSpot().getIntervalConfigs().clear();

    KlineSetKey key = new KlineSetKey("BTCUSDT", IntervalEnum.ONE_HOUR.code());
    assertEquals(Set.of(), optedOut.invokeRestorePersistedKlines(Set.of(key)));
    optedOut.updateKlines("ETHUSDT", IntervalEnum.ONE_HOUR.code(),
        List.of(optedOut.buildServerKline(BASE_TIME, "800.0", 10)));
    optedOut.setServerTime(BASE_TIME + 2 * HOUR);
    optedOut.invokeDumpPersistedKlines(true);
    optedOut.invokeReconcilePersistedKlines(Set.of(new KlineSetKey("ETHUSDT", IntervalEnum.ONE_HOUR.code())));
    assertFalse(Files.exists(tempDir.resolve("spot").resolve("1h").resolve("ETHUSDT")),
        "opted-out interval was persisted");
  }

  private NavigableMap<Long, Object[]> displayKlines(HarnessKlineService service, String symbol) {
    KlineSet klineSet = service.klineSetMap.get(new KlineSetKey(symbol, IntervalEnum.ONE_HOUR.code()));
    assertNotNull(klineSet, "kline set missing for " + symbol);
    NavigableMap<Long, Object[]> display = new TreeMap<>();
    klineSet.getKlineMap().forEach((openTime, kline) ->
        display.put(openTime, ConvertUtil.convertToDisplayKline(kline)));
    return display;
  }

  private JsonKlinePersistenceStore buildStore() {
    KlinePersistenceProperties properties = new KlinePersistenceProperties();
    properties.setRootDir(tempDir.toString());
    return new JsonKlinePersistenceStore(properties, new Serializer(new ObjectMapper()));
  }

  static class HarnessKlineService extends AbstractKlineService<WebSocketClient> {

    private final KlineSyncConfigProperties syncConfig;

    private volatile long serverTime = System.currentTimeMillis();

    private final List<QueryKlineRequest> queryRequests = new ArrayList<>();

    private List<Kline> anyQueryResult = List.of();

    record QueryKlineRequest(String symbol, String interval, Long startTime, Long endTime, Integer limit) {
    }

    HarnessKlineService(String numberType, JsonKlinePersistenceStore store) {
      this(numberType, store, 500);
    }

    HarnessKlineService(String numberType, JsonKlinePersistenceStore store, int minMaintainCount) {
      IntervalSyncConfig intervalSyncConfig = new IntervalSyncConfig();
      intervalSyncConfig.setMinMaintainCount(minMaintainCount);
      intervalSyncConfig.setListenSymbolPatterns(List.of(".*USDT"));
      syncConfig = new KlineSyncConfigProperties();
      syncConfig.setIntervalSyncConfigs(Map.of(IntervalEnum.ONE_HOUR.code(), intervalSyncConfig));
      this.rateLimitManager = mock(RateLimitManager.class);
      ReflectionTestUtils.setField(this, "numberType", numberType);
      KlinePersistenceProperties persistenceProperties = new KlinePersistenceProperties();
      persistenceProperties.setEnabled(true);
      KlinePersistenceProperties.IntervalPersistenceConfig intervalPersistenceConfig =
          new KlinePersistenceProperties.IntervalPersistenceConfig();
      persistenceProperties.getSpot().getIntervalConfigs()
          .put(IntervalEnum.ONE_HOUR.code(), intervalPersistenceConfig);
      ReflectionTestUtils.setField(this, "persistenceProperties", persistenceProperties);
      ReflectionTestUtils.setField(this, "klinePersistenceStore", store);
    }

    Kline buildServerKline(long openTime, String closePrice, int tradeNum) {
      return serverKlineToKline(new Object[] {
          openTime, closePrice, closePrice, closePrice, closePrice, "12.5",
          openTime + HOUR - 1, "1000.25", tradeNum, "6.25", "500.125"
      });
    }

    void setServerTime(long serverTime) {
      this.serverTime = serverTime;
    }

    @SuppressWarnings("unchecked")
    void putSymbolOnboardTime(String symbol, long onboardTime) {
      Map<String, Long> symbolOnboardTimeMap =
          (Map<String, Long>) ReflectionTestUtils.getField(this, "symbolOnboardTimeMap");
      assertNotNull(symbolOnboardTimeMap);
      symbolOnboardTimeMap.put(symbol, onboardTime);
    }

    @SuppressWarnings("unchecked")
    Set<KlineSetKey> invokeRestorePersistedKlines(Set<KlineSetKey> configuredKlineSetKeys) {
      return (Set<KlineSetKey>) ReflectionTestUtils.invokeMethod(
          this, "restorePersistedKlines", configuredKlineSetKeys);
    }

    void invokeWarmUpPersistedKlines(Set<KlineSetKey> restoredKlineSetKeys) {
      ReflectionTestUtils.invokeMethod(this, "warmUpPersistedKlines", restoredKlineSetKeys);
    }

    void invokeDumpPersistedKlines(boolean force) {
      ReflectionTestUtils.invokeMethod(this, "dumpPersistedKlines", force);
    }

    void invokeReconcilePersistedKlines(Set<KlineSetKey> configuredKlineSetKeys) {
      ReflectionTestUtils.invokeMethod(this, "reconcilePersistedKlines", configuredKlineSetKeys);
    }

    void stubAnyQueryResult(List<Kline> klines) {
      this.anyQueryResult = List.copyOf(klines);
    }

    List<QueryKlineRequest> getQueryRequests() {
      return List.copyOf(queryRequests);
    }

    @Override
    protected List<Kline> queryKlines0(String symbol, String interval, Long startTime, Long endTime, Integer limit) {
      queryRequests.add(new QueryKlineRequest(symbol, interval, startTime, endTime, limit));
      return anyQueryResult;
    }

    @Override
    protected List<Ticker24Hr> queryTicker24Hrs0() {
      return List.of();
    }

    @Override
    protected List<Ticker<?>> queryTickers0() {
      return List.of();
    }

    @Override
    protected String getRateLimiterName() {
      return "test";
    }

    @Override
    protected List<String> getSymbols() {
      return List.of("BTCUSDT", "ETHUSDT");
    }

    @Override
    protected KlineSyncConfigProperties getSyncConfig() {
      return syncConfig;
    }

    @Override
    protected int getMakeUpKlinesLimit() {
      return 500;
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
  }
}
