package com.zx.quant.klineproxy.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zx.quant.klineproxy.client.BinanceFutureClient;
import com.zx.quant.klineproxy.client.BinanceSpotClient;
import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.KlineSet;
import com.zx.quant.klineproxy.model.KlineSetKey;
import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties.BinanceFutureKlineSyncConfigProperties;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties.BinanceSpotKlineSyncConfigProperties;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties.IntervalSyncConfig;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties.IntervalSyncFutureConfig;
import com.zx.quant.klineproxy.model.constant.Constants;
import com.zx.quant.klineproxy.monitor.MonitorManager;
import com.zx.quant.klineproxy.service.ExchangeService;
import com.zx.quant.klineproxy.util.ConvertUtil;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;
import retrofit2.Call;
import retrofit2.Response;

class SingleTickerMemoryFirstTest {

  private static final long HOUR = 3_600_000L;
  private static final long NOW = 1_790_622_123_000L;
  private static final long OPEN = NOW / HOUR * HOUR;
  private static final String SYMBOL = "BTCUSDT";

  static Stream<Arguments> marketsAndNumberTypes() {
    return Stream.of(true, false).flatMap(future ->
        Stream.of("string", "float", "double", "bigDecimal")
            .map(type -> Arguments.of(future, type)));
  }

  @ParameterizedTest
  @MethodSource("marketsAndNumberTypes")
  void cachedPriceBypassesRestAndLimiterAndImmediatelyReflectsStreamUpdates(boolean future, String type) {
    Fixture f = fixture(future, type);
    f.service.updateStreamKline(SYMBOL, "1h", bar(f, OPEN, "123.4500", 1), false);

    List<Ticker<?>> first = f.service.queryTickers(List.of(SYMBOL));
    assertThat(first).hasSize(1);
    assertThat(first.getFirst().getSymbol()).isEqualTo(SYMBOL);
    assertThat(first.getFirst().getTime()).isEqualTo(NOW);
    assertThat(new BigDecimal(first.getFirst().getPrice().toString()))
        .isEqualByComparingTo("123.45");
    if (type.equals("string") || type.equals("bigDecimal")) {
      assertThat(first.getFirst().getPrice().toString()).isEqualTo("123.4500");
    }

    f.service.updateStreamKline(SYMBOL, "1h", bar(f, OPEN, "124.5600", 2), false);
    assertThat(new BigDecimal(f.service.queryTickers(List.of(SYMBOL)).getFirst().getPrice().toString()))
        .isEqualByComparingTo("124.56");
    verifyNoInteractions(f.limiter, f.futureClient, f.spotClient);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void readsAnotherSubscribedIntervalWhileTheFirstIsStillEmpty(boolean future) {
    Fixture f = fixture(future, "bigDecimal");
    KlineSetKey emptyKey = new KlineSetKey(SYMBOL, "1h");
    f.service.klineSetMap.put(emptyKey, new KlineSet(emptyKey));
    long dayOpen = NOW / (24 * HOUR) * (24 * HOUR);
    Kline day = bar(f, dayOpen, "456.7800", 1);
    day.setCloseTime(dayOpen + 24 * HOUR - 1);
    f.service.updateStreamKline(SYMBOL, "1d", day, false);

    assertThat(f.service.queryTickers(List.of(SYMBOL)).getFirst().getPrice())
        .isEqualTo(new BigDecimal("456.7800"));
    assertThat(f.service.klineSetMap.get(emptyKey).getKlineMap()).isEmpty();
    verifyNoInteractions(f.limiter, f.futureClient, f.spotClient);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void missingSymbolUsesRestAfterAcquiringTheExistingWeight(boolean future) throws IOException {
    Fixture f = fixture(future, "bigDecimal");
    // Other symbols being cached must never count as a hit for the requested symbol.
    f.service.updateStreamKline("ETHUSDT", "1h", bar(f, OPEN, "123.45", 1), false);
    Ticker.BigDecimalTicker rest = restTicker();
    Call<Ticker.BigDecimalTicker> call = f.stubRest(Response.success(rest));

    assertThat(f.service.queryTickers(List.of(SYMBOL))).containsExactly(rest);
    var order = inOrder(f.limiter, f.client(), call);
    order.verify(f.limiter).acquire(f.limiterName(), future ? 1 : 2);
    if (future) {
      order.verify(f.futureClient).getSymbolTickerPrice(SYMBOL);
    } else {
      order.verify(f.spotClient).getSymbolTickerPrice(SYMBOL);
    }
    order.verify(call).execute();
    assertThat(f.service.klineSetMap).containsOnlyKeys(new KlineSetKey("ETHUSDT", "1h"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void emptyCachedSeriesStillFallsBackToRest(boolean future) throws IOException {
    Fixture f = fixture(future, "bigDecimal");
    KlineSetKey key = new KlineSetKey(SYMBOL, "1h");
    f.service.klineSetMap.put(key, new KlineSet(key));
    Ticker.BigDecimalTicker rest = restTicker();
    Call<Ticker.BigDecimalTicker> call = f.stubRest(Response.success(rest));

    assertThat(f.service.queryTickers(List.of(SYMBOL))).containsExactly(rest);
    verify(f.limiter).acquire(f.limiterName(), future ? 1 : 2);
    verify(call).execute();
    assertThat(f.service.klineSetMap.get(key).getKlineMap()).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void restFailureRemainsVisibleOnACacheMiss(boolean future) throws IOException {
    Fixture f = fixture(future, "bigDecimal");
    Call<Ticker.BigDecimalTicker> call = f.stubRest(Response.success(restTicker()));
    when(call.execute()).thenThrow(new IOException("upstream unavailable"));

    assertThrows(RuntimeException.class, () -> f.service.queryTickers(List.of(SYMBOL)));
    verify(f.limiter).acquire(f.limiterName(), future ? 1 : 2);
    verify(call).execute();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void preservesMarketSpecificPriceResponseShape(boolean future) {
    Fixture f = fixture(future, "bigDecimal");
    f.service.updateStreamKline(SYMBOL, "1h", bar(f, OPEN, "123.4500", 1), false);

    JsonNode display = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL)
        .valueToTree(ConvertUtil.convertToDisplayTicker(
            f.service.queryTickers(List.of(SYMBOL)), false, future));
    assertThat(display.path("symbol").asText()).isEqualTo(SYMBOL);
    assertThat(display.path("price").asText()).isEqualTo("123.4500");
    if (future) {
      assertThat(display.path("time").asLong()).isEqualTo(NOW);
    } else {
      assertThat(display.has("time")).isFalse();
    }
    verifyNoInteractions(f.limiter, f.futureClient, f.spotClient);
  }

  private static Kline bar(Fixture f, long open, String price, int trades) {
    return f.service.serverKlineToKline(new Object[] {
        open, price, price, price, price, "1", open + HOUR - 1, "1", trades, "1", "1", "0"
    });
  }

  private static Ticker.BigDecimalTicker restTicker() {
    Ticker.BigDecimalTicker ticker = new Ticker.BigDecimalTicker();
    ticker.setSymbol(SYMBOL);
    ticker.setPrice(new BigDecimal("987.65432100"));
    ticker.setTime(NOW - 10);
    return ticker;
  }

  private static Fixture fixture(boolean future, String type) {
    AbstractKlineService<?> service;
    BinanceFutureClient futureClient = mock(BinanceFutureClient.class);
    BinanceSpotClient spotClient = mock(BinanceSpotClient.class);
    if (future) {
      service = new BinanceFutureKlineServiceImpl();
      var config = new BinanceFutureKlineSyncConfigProperties();
      Map<String, IntervalSyncFutureConfig> intervals = new LinkedHashMap<>();
      intervals.put("1h", new IntervalSyncFutureConfig());
      intervals.put("1d", new IntervalSyncFutureConfig());
      config.setIntervalSyncConfigs(intervals);
      ReflectionTestUtils.setField(service, "klineSyncConfigProperties", config);
      ReflectionTestUtils.setField(service, "binanceFutureClient", futureClient);
    } else {
      service = new BinanceSpotKlineServiceImpl();
      var config = new BinanceSpotKlineSyncConfigProperties();
      Map<String, IntervalSyncConfig> intervals = new LinkedHashMap<>();
      intervals.put("1h", new IntervalSyncConfig());
      intervals.put("1d", new IntervalSyncConfig());
      config.setIntervalSyncConfigs(intervals);
      ReflectionTestUtils.setField(service, "klineSyncConfigProperties", config);
      ReflectionTestUtils.setField(service, "binanceSpotClient", spotClient);
    }
    ExchangeService<?> exchange = mock(ExchangeService.class);
    when(exchange.queryServerTime()).thenReturn(NOW);
    RateLimitManager limiter = mock(RateLimitManager.class);
    ReflectionTestUtils.setField(service, "exchangeService", exchange);
    ReflectionTestUtils.setField(service, "rateLimitManager", limiter);
    ReflectionTestUtils.setField(service, "monitorManager", mock(MonitorManager.class));
    ReflectionTestUtils.setField(service, "numberType", type);
    return new Fixture(future, service, futureClient, spotClient, limiter);
  }

  private record Fixture(boolean future, AbstractKlineService<?> service,
                         BinanceFutureClient futureClient, BinanceSpotClient spotClient,
                         RateLimitManager limiter) {
    Object client() {
      return future ? futureClient : spotClient;
    }

    String limiterName() {
      return future ? Constants.BINANCE_FUTURE_KLINES_FETCHER_RATE_LIMITER_NAME
          : Constants.BINANCE_SPOT_KLINES_FETCHER_RATE_LIMITER_NAME;
    }

    @SuppressWarnings("unchecked")
    Call<Ticker.BigDecimalTicker> stubRest(Response<Ticker.BigDecimalTicker> response) throws IOException {
      Call<Ticker.BigDecimalTicker> call = mock(Call.class);
      if (future) {
        when(futureClient.getSymbolTickerPrice(SYMBOL)).thenReturn(call);
      } else {
        when(spotClient.getSymbolTickerPrice(SYMBOL)).thenReturn(call);
      }
      when(call.execute()).thenReturn(response);
      return call;
    }
  }
}
