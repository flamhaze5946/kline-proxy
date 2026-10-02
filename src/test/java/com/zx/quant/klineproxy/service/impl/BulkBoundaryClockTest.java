package com.zx.quant.klineproxy.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.zx.quant.klineproxy.client.model.BinanceFutureExchange;
import com.zx.quant.klineproxy.client.model.BinanceSpotExchange;
import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.model.BulkKlinesResponse;
import com.zx.quant.klineproxy.model.Kline.StringKline;
import com.zx.quant.klineproxy.model.config.KlineBulkProperties;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties.BinanceFutureKlineSyncConfigProperties;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties.BinanceSpotKlineSyncConfigProperties;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties.IntervalSyncConfig;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties.IntervalSyncFutureConfig;
import com.zx.quant.klineproxy.model.enums.IntervalEnum;
import com.zx.quant.klineproxy.monitor.MonitorManager;
import com.zx.quant.klineproxy.service.ExchangeService;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Bulk closed_only boundary decisions (docs/boundary-clock-bias-20260930.md). On 2026-09-30 at
 * 15:00Z the proxy's exchange-time estimate trailed true time by about half the 20-25 ms round
 * trip to fapi. Requests arriving at T+0..4 ms were answered at once, and the 14:00 bar was
 * missing from their answer. Both clocks are pinned here. The host clock stands for true time and
 * the exchange estimate trails it by {@link #LAG_MS}.
 */
class BulkBoundaryClockTest {

  private static final long H = IntervalEnum.ONE_HOUR.getMills();

  /** 2026-09-30 15:00:00Z, the hour of the incident */
  private static final long T = Instant.parse("2026-09-30T15:00:00Z").toEpochMilli();

  /** half the measured 20-25 ms round trip to fapi */
  private static final long LAG_MS = 10L;

  private static final String SYMBOL = "BTCUSDT";

  enum Market { FUTURE, SPOT }

  /** (a) the incident: 5 ms after the hour, as seen through a trailing exchange clock */
  @ParameterizedTest
  @EnumSource(Market.class)
  void justAfterTheBoundaryATrailingExchangeClockStillWaitsForTheClosingBar(Market market)
      throws Exception {
    // no pre-boundary wait: the boundary clock alone must see the hour as passed
    Fixture f = fixture(market, 0L, 8_000L);
    f.at(T - 5_000);
    f.seedFormingHour();
    BulkKlinesResponse before = f.bulk(true);
    assertThat(lastRow(before)[0]).isEqualTo(T - 2 * H);
    assertThat(before.finalized()).isTrue();  // cached under the 14:00 boundary

    f.at(T + 5);
    assertThat(f.clocks.server).isLessThan(T);  // the exchange estimate still says 14:59:59.995
    Thread closer = f.closeLater(150);
    BulkKlinesResponse after = f.bulk(true);
    closer.join();

    assertThat(after).isNotSameAs(before);
    assertThat(lastRow(after)[0]).isEqualTo(T - H);
    assertThat(lastRow(after)[4]).isEqualTo("final");
    assertThat(after.finalized()).isTrue();
    assertThat(after.pending()).isEmpty();
    assertThat(after.waitedMs()).isGreaterThanOrEqualTo(100L);
    assertThat(after.tsMs()).isGreaterThanOrEqualTo(T);
  }

  /** kline.bulk.hostClockBoundary=false: the pre-fix server-time decision, cached snapshot included */
  @ParameterizedTest
  @EnumSource(Market.class)
  void theSwitchOffRestoresTheServerTimeBoundaryDecision(Market market) {
    Fixture f = fixture(market, 0L, 8_000L, false);
    f.at(T - 5_000);
    f.seedFormingHour();
    BulkKlinesResponse before = f.bulk(true);

    f.at(T + 5);
    long started = System.nanoTime();
    BulkKlinesResponse after = f.bulk(true);

    assertThat((System.nanoTime() - started) / 1_000_000L).isLessThan(1_000L);
    assertThat(after).isSameAs(before);
    assertThat(lastRow(after)[0]).isEqualTo(T - 2 * H);
    assertThat(after.waitedMs()).isZero();
    assertThat(after.tsMs()).isEqualTo(T - 5_000 - LAG_MS);
  }

  @Test
  void hostClockBoundaryBindsAndDefaultsToOn() {
    assertThat(new KlineBulkProperties().isHostClockBoundary()).isTrue();
    KlineBulkProperties bound = new Binder(new MapConfigurationPropertySource(
        Map.of("kline.bulk.hostClockBoundary", "false"))).bind("kline.bulk",
        Bindable.of(KlineBulkProperties.class)).get();
    assertThat(bound.isHostClockBoundary()).isFalse();
  }

  /** (b) 100 ms early: wait for the boundary, then for the closing update */
  @Test
  void aRequestJustBeforeTheBoundaryWaitsForItThenForTheClosingBar() throws Exception {
    Fixture f = fixture(Market.FUTURE, 250L, 8_000L);
    f.at(T - 100);
    f.seedFormingHour();
    Thread closer = f.closeLater(300);
    long started = System.nanoTime();
    BulkKlinesResponse response = f.bulk(true);
    long elapsedMs = (System.nanoTime() - started) / 1_000_000L;
    closer.join();

    assertThat(lastRow(response)[0]).isEqualTo(T - H);
    assertThat(lastRow(response)[4]).isEqualTo("final");
    assertThat(response.finalized()).isTrue();
    assertThat(response.pending()).isEmpty();
    // the final wait starts at the boundary, after the ~100 ms pre-boundary sleep
    assertThat(response.waitedMs()).isBetween(50L, 4_000L);
    assertThat(elapsedMs).isGreaterThanOrEqualTo(250L);
  }

  /** (b) the final-wait cap still runs from the boundary, not from the early arrival */
  @Test
  void theCapAfterAPreBoundaryWaitIsMeasuredFromTheBoundary() {
    Fixture f = fixture(Market.FUTURE, 250L, 300L);
    f.at(T - 100);
    f.seedFormingHour();  // the closing update never arrives
    BulkKlinesResponse response = f.bulk(true);

    assertThat(lastRow(response)[0]).isEqualTo(T - H);
    assertThat(response.finalized()).isFalse();
    assertThat(response.pending()).containsExactly(SYMBOL);
    assertThat(response.waitedMs()).isBetween(250L, 2_000L);
  }

  /** (c) seconds early: unchanged, the previous hour at once */
  @Test
  void aRequestSecondsBeforeTheBoundaryIsAnsweredAtOnceFromThePreviousHour() {
    Fixture f = fixture(Market.FUTURE, 250L, 8_000L);
    f.at(T - 5_000);
    f.seedFormingHour();
    long started = System.nanoTime();
    BulkKlinesResponse response = f.bulk(true);
    long elapsedMs = (System.nanoTime() - started) / 1_000_000L;

    assertThat(elapsedMs).isLessThan(1_000L);
    assertThat(lastRow(response)[0]).isEqualTo(T - 2 * H);
    assertThat(response.finalized()).isTrue();
    assertThat(response.pending()).isEmpty();
    assertThat(response.waitedMs()).isZero();
  }

  /** closed_only=false has no finality contract and never waits for a boundary */
  @Test
  void closedOnlyFalseDoesNotWaitForTheBoundary() {
    Fixture f = fixture(Market.FUTURE, 2_000L, 8_000L);
    f.at(T - 1_500);
    f.seedFormingHour();
    long started = System.nanoTime();
    BulkKlinesResponse response = f.bulk(false);
    long elapsedMs = (System.nanoTime() - started) / 1_000_000L;

    assertThat(elapsedMs).isLessThan(1_000L);
    assertThat(lastRow(response)[0]).isEqualTo(T - H);
    assertThat(lastRow(response)[4]).isEqualTo("forming");
    assertThat(response.waitedMs()).isZero();
  }

  /** A host clock that falls behind is floored by the exchange estimate. */
  @Test
  void aHostClockBehindTheExchangeEstimateDoesNotDelayTheBoundary() throws Exception {
    Fixture f = fixture(Market.FUTURE, 0L, 8_000L);
    f.clocks.host = T - 2_000;
    f.clocks.server = T + 5;
    f.seedFormingHour();
    Thread closer = f.closeLater(150);
    BulkKlinesResponse response = f.bulk(true);
    closer.join();

    assertThat(lastRow(response)[0]).isEqualTo(T - H);
    assertThat(lastRow(response)[4]).isEqualTo("final");
    assertThat(response.finalized()).isTrue();
  }

  @Test
  void preBoundaryWaitBindsDefaultsTo250AndIsCapped() {
    assertThat(new KlineBulkProperties().effectivePreBoundaryWaitMs()).isEqualTo(250L);
    KlineBulkProperties bound = new Binder(new MapConfigurationPropertySource(
        Map.of("kline.bulk.preBoundaryWaitMs", "120"))).bind("kline.bulk",
        Bindable.of(KlineBulkProperties.class)).get();
    assertThat(bound.effectivePreBoundaryWaitMs()).isEqualTo(120L);
    bound.setPreBoundaryWaitMs(60_000L);
    assertThat(bound.effectivePreBoundaryWaitMs()).isEqualTo(2_000L);
    bound.setPreBoundaryWaitMs(-1L);
    assertThat(bound.effectivePreBoundaryWaitMs()).isZero();
  }

  private static Object[] lastRow(BulkKlinesResponse response) {
    return response.klines().get(SYMBOL).getLast();
  }

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

  private static final class Clocks {
    private volatile long host;
    private volatile long server;
  }

  private record Fixture(AbstractKlineService<?> service, Clocks clocks) {

    /** true time {@code host}; the exchange estimate trails it */
    void at(long host) {
      clocks.host = host;
      clocks.server = host - LAG_MS;
    }

    /** 13:00 final; 14:00 still forming (websocket x=false) */
    void seedFormingHour() {
      service.updateStreamKline(SYMBOL, "1h", hourBar(T - 2 * H, 10, "prior"), true);
      service.updateStreamKline(SYMBOL, "1h", hourBar(T - H, 11, "forming"), false);
    }

    /** the 14:00 closing update (x=true), from another thread after {@code delayMs} */
    Thread closeLater(long delayMs) {
      Thread closer = new Thread(() -> {
        try {
          Thread.sleep(delayMs);
        } catch (InterruptedException e) {
          return;
        }
        service.updateStreamKline(SYMBOL, "1h", hourBar(T - H, 12, "final"), true);
      });
      closer.start();
      return closer;
    }

    BulkKlinesResponse bulk(boolean closedOnly) {
      return service.queryBulkKlines("1h", 2, closedOnly, List.of(SYMBOL));
    }
  }

  private static Fixture fixture(Market market, long preBoundaryWaitMs, long finalWaitMaxMs) {
    return fixture(market, preBoundaryWaitMs, finalWaitMaxMs, true);
  }

  @SuppressWarnings("unchecked")
  private static Fixture fixture(Market market, long preBoundaryWaitMs, long finalWaitMaxMs,
      boolean hostClockBoundary) {
    Clocks clocks = new Clocks();
    AbstractKlineService<?> service;
    if (market == Market.FUTURE) {
      BinanceFutureKlineServiceImpl future = new BinanceFutureKlineServiceImpl() {
        @Override
        protected long getHostTime() {
          return clocks.host;
        }
      };
      BinanceFutureKlineSyncConfigProperties config = new BinanceFutureKlineSyncConfigProperties();
      IntervalSyncFutureConfig hour = new IntervalSyncFutureConfig();
      hour.setListenSymbolPatterns(List.of(".*"));
      config.setIntervalSyncConfigs(Map.of("1h", hour));
      ExchangeService<BinanceFutureExchange> exchange = mock(ExchangeService.class);
      when(exchange.queryServerTime()).thenAnswer(invocation -> clocks.server);
      ReflectionTestUtils.setField(future, "klineSyncConfigProperties", config);
      ReflectionTestUtils.setField(future, "exchangeService", exchange);
      service = future;
    } else {
      BinanceSpotKlineServiceImpl spot = new BinanceSpotKlineServiceImpl() {
        @Override
        protected long getHostTime() {
          return clocks.host;
        }
      };
      BinanceSpotKlineSyncConfigProperties config = new BinanceSpotKlineSyncConfigProperties();
      IntervalSyncConfig hour = new IntervalSyncConfig();
      hour.setListenSymbolPatterns(List.of(".*"));
      config.setIntervalSyncConfigs(Map.of("1h", hour));
      ExchangeService<BinanceSpotExchange> exchange = mock(ExchangeService.class);
      when(exchange.queryServerTime()).thenAnswer(invocation -> clocks.server);
      ReflectionTestUtils.setField(spot, "klineSyncConfigProperties", config);
      ReflectionTestUtils.setField(spot, "exchangeService", exchange);
      service = spot;
    }
    KlineBulkProperties bulk = new KlineBulkProperties();
    bulk.setPreBoundaryWaitMs(preBoundaryWaitMs);
    bulk.setFinalWaitMaxMs(finalWaitMaxMs);
    bulk.setHostClockBoundary(hostClockBoundary);
    ReflectionTestUtils.setField(service, "bulkProperties", bulk);
    ReflectionTestUtils.setField(service, "rateLimitManager", mock(RateLimitManager.class));
    ReflectionTestUtils.setField(service, "monitorManager", mock(MonitorManager.class));
    ReflectionTestUtils.setField(service, "numberType", "string");
    return new Fixture(service, clocks);
  }
}
