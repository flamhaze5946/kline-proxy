package com.zx.quant.klineproxy.service.impl;

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
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties.IntervalSyncConfig;
import com.zx.quant.klineproxy.model.enums.IntervalEnum;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

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
  void updateKlinesShouldTrimSeriesWhenMaintainCountExceeded() {
    TestKlineService service = new TestKlineService();
    List<Kline> klines = java.util.stream.IntStream.range(0, 60)
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

  private static class TestKlineService extends AbstractKlineService<WebSocketClient> {

    private final KlineSyncConfigProperties syncConfig;

    private List<String> symbols = List.of();

    private List<Ticker<?>> realtimeTickers = List.of();

    private List<Ticker24Hr> realtimeTicker24Hrs = List.of();

    private final AtomicInteger realtimeTickerQueryCount = new AtomicInteger(0);

    private final AtomicInteger realtimeTicker24HrQueryCount = new AtomicInteger(0);

    private volatile long realtimeTickerDelayMillis;

    private volatile long realtimeTicker24HrDelayMillis;

    private TestKlineService() {
      IntervalSyncConfig intervalSyncConfig = new IntervalSyncConfig();
      intervalSyncConfig.setMinMaintainCount(3);
      intervalSyncConfig.setListenSymbolPatterns(List.of(".*USDT"));
      syncConfig = new KlineSyncConfigProperties();
      syncConfig.setIntervalSyncConfigs(Map.of(IntervalEnum.ONE_HOUR.code(), intervalSyncConfig));
      this.rateLimitManager = mock(RateLimitManager.class);
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

    @Override
    protected List<Kline> queryKlines0(String symbol, String interval, Long startTime, Long endTime, Integer limit) {
      return List.of();
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
