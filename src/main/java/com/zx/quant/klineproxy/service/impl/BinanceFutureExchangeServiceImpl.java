package com.zx.quant.klineproxy.service.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.zx.quant.klineproxy.client.BinanceFutureClient;
import com.zx.quant.klineproxy.client.model.BinanceFutureExchange;
import com.zx.quant.klineproxy.client.model.BinanceFutureSymbol;
import com.zx.quant.klineproxy.client.model.BinanceServerTime;
import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.model.BulkFundingRateResponse;
import com.zx.quant.klineproxy.model.FutureFundingRate;
import com.zx.quant.klineproxy.model.FuturePremiumIndex;
import com.zx.quant.klineproxy.model.constant.Constants;
import com.zx.quant.klineproxy.service.FutureExchangeService;
import com.zx.quant.klineproxy.util.ClientUtil;
import com.zx.quant.klineproxy.util.ConvertUtil;
import com.zx.quant.klineproxy.util.ConvertUtil.DisplayFundingRate;
import com.zx.quant.klineproxy.util.ExceptionSafeRunnable;
import com.zx.quant.klineproxy.util.ThreadFactoryUtil;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import retrofit2.Call;

/**
 * binance future exchange service impl
 * @author flamhaze5946
 */
@Service("binanceFutureExchangeService")
public class BinanceFutureExchangeServiceImpl implements FutureExchangeService<BinanceFutureExchange>, InitializingBean {

  private static final String VALID_SYMBOL_STATUS = "TRADING";

  private static final String SERVER_TIME_REFRESHER_GROUP = "futureServerTimeRefresher";

  private static final int DEFAULT_BULK_FUNDING_LIMIT = 1;

  private static final int MAX_BULK_FUNDING_LIMIT = 100;

  private static final long FUNDING_INTERVAL_MS = 8L * 60L * 60L * 1000L;

  private final ScheduledExecutorService serverTimeRefresher = new ScheduledThreadPoolExecutor(1,
      ThreadFactoryUtil.getNamedThreadFactory(SERVER_TIME_REFRESHER_GROUP));

  private final LoadingCache<String, BinanceFutureExchange> exchangeCache = buildExchangeCache();

  private final Cache<RecentFundingKey, BulkFundingRateResponse> bulkFundingRecentCache = Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofSeconds(60))
      .maximumSize(64)
      .build();

  private final Cache<HistoricalFundingKey, DisplayFundingRate> bulkFundingHistoricalCache = Caffeine.newBuilder()
      .maximumSize(200_000)
      .build();

  private final AtomicLong serverTimeDelta = new AtomicLong(0);

  @Autowired
  private BinanceFutureClient binanceFutureClient;

  @Autowired
  private RateLimitManager rateLimitManager;

  @Override
  public void afterPropertiesSet() throws Exception {
    refreshServerTimeDelta();
    serverTimeRefresher.scheduleAtFixedRate(new ExceptionSafeRunnable(this::refreshServerTimeDelta), 5, 3600, TimeUnit.SECONDS);
  }

  @Override
  public BinanceFutureExchange queryExchange() {
    BinanceFutureExchange exchange = exchangeCache.get(StringUtils.EMPTY);
    exchange.setServerTime(queryServerTime());
    return exchange;
  }

  @Override
  public long queryServerTime() {
    return System.currentTimeMillis() - serverTimeDelta.get();
  }

  @Override
  public List<FutureFundingRate> queryFundingRates(String symbol, Long startTime, Long endTime, Integer limit) {
    rateLimitManager.acquire(Constants.BINANCE_FUTURE_KLINES_FETCHER_RATE_LIMITER_NAME, 1);
    Call<List<FutureFundingRate>> ratesCall = binanceFutureClient.getFundingRates(symbol, startTime, endTime, limit);
    List<FutureFundingRate> rates = ClientUtil.getResponseBody(ratesCall,
        () -> rateLimitManager.stopAcquire(Constants.BINANCE_FUTURE_KLINES_FETCHER_RATE_LIMITER_NAME, 1000 * 30));
    return rates == null ? List.of() : rates;
  }

  @Override
  public BulkFundingRateResponse queryBulkFundingRates(Collection<String> symbols, Long sinceMs, Long untilMs, Integer limit) {
    int realLimit = Math.min(Math.max(limit != null ? limit : DEFAULT_BULK_FUNDING_LIMIT, 1),
        MAX_BULK_FUNDING_LIMIT);
    // R31-perf: when caller doesn't specify symbols, use Binance's
    // single no-symbol /fapi/v1/fundingRate call (limit up to 1000) to
    // fetch all funding events in the window in ONE round trip instead
    // of looping 608+ symbols sequentially (which took ~49s). For
    // nos-rs scraper hot path this drops the funding bulk call from
    // ~49s to ~200ms.
    boolean noSymbolsSpecified = symbols == null || symbols.isEmpty();
    if (noSymbolsSpecified && sinceMs != null && untilMs != null) {
      return loadBulkFundingRatesNoSymbol(sinceMs, untilMs, realLimit);
    }
    List<String> realSymbols = normalizeFundingSymbols(symbols);
    if (sinceMs == null && untilMs == null) {
      // R31-fix CRIT 2: include the current 8h funding boundary in the cache
      // key so the 60s TTL window cannot serve a stale entry across a
      // funding boundary. Without this, an entry populated 30s before the
      // boundary would return the previous-event payload to the first
      // post-boundary request, causing Rust scraper to write fr=0 on the
      // funding candle.
      long fundingBoundaryMs = Math.floorDiv(System.currentTimeMillis(), FUNDING_INTERVAL_MS) * FUNDING_INTERVAL_MS;
      RecentFundingKey key = new RecentFundingKey(realSymbols, realLimit, fundingBoundaryMs);
      return bulkFundingRecentCache.get(key, ignored -> loadBulkFundingRates(realSymbols, null, null, realLimit));
    }
    return loadBulkFundingRatesWithHistoricalCache(realSymbols, sinceMs, untilMs, realLimit);
  }

  /**
   * R31-perf: single Binance fundingRate call (no symbol param) to fetch
   * every symbol's funding events in [sinceMs, untilMs) within one HTTP
   * round-trip. Binance supports symbol=null + startTime + endTime + limit
   * (up to 1000); response is sorted by fundingTime. Output is grouped by
   * symbol with per-symbol lists truncated to {@code limit}.
   */
  private BulkFundingRateResponse loadBulkFundingRatesNoSymbol(long sinceMs, long untilMs, int limit) {
    // Use Binance's MAX_BULK_FUNDING_LIMIT for the underlying fetch
    // (covers ~all symbols' single-event-per-window case) but each
    // per-symbol bucket below is still capped at the caller's limit.
    final int FETCH_LIMIT = 1000;
    List<FutureFundingRate> rows = queryFundingRates(null, sinceMs, untilMs, FETCH_LIMIT).stream()
        .filter(rate -> rate.getFundingTime() != null)
        .filter(rate -> rate.getSymbol() != null && !rate.getSymbol().isBlank())
        .filter(rate -> rate.getFundingTime() >= sinceMs)
        .filter(rate -> rate.getFundingTime() < untilMs)
        .sorted((left, right) -> left.getFundingTime().compareTo(right.getFundingTime()))
        .toList();
    Map<String, List<DisplayFundingRate>> out = new LinkedHashMap<>();
    for (FutureFundingRate row : rows) {
      String symbol = row.getSymbol();
      DisplayFundingRate display = ConvertUtil.convertToDisplayFundingRate(row);
      if (display.getFundingTime() != null) {
        bulkFundingHistoricalCache.put(new HistoricalFundingKey(symbol, display.getFundingTime()), display);
      }
      out.computeIfAbsent(symbol, ignored -> new ArrayList<>()).add(display);
    }
    // Cap each symbol's list to the caller's limit (keep latest).
    out.replaceAll((sym, list) -> list.size() > limit ? new ArrayList<>(list.subList(list.size() - limit, list.size())) : list);
    return new BulkFundingRateResponse(System.currentTimeMillis(), out);
  }

  private BulkFundingRateResponse loadBulkFundingRatesWithHistoricalCache(
      List<String> symbols, Long sinceMs, Long untilMs, int limit) {
    // R31-fix-r2: explicit window queries skip the historical-cache
    // expectedFundingTimes derivation. That derivation assumes
    // FUNDING_INTERVAL_MS=8h (line 194) and produces a wrong "expected"
    // time set for variable-interval funding symbols (e.g. 1h funding).
    // For nos-rs's hot path the in-cycle window is always (target_cbt,
    // target_cbt+HOUR_MS+1) — Binance returns at most one event per
    // symbol per call regardless of interval, and the per-call cost is
    // small. The historical cache is still WRITTEN by loadBulkFundingRates
    // (line 169) for any future caller that wants point lookups via
    // (symbol, fundingTime), but explicit windows no longer try to
    // reconstruct expected timestamps a priori.
    return loadBulkFundingRates(symbols, sinceMs, untilMs, limit);
  }

  private BulkFundingRateResponse loadBulkFundingRates(List<String> symbols, Long sinceMs, Long untilMs, int limit) {
    Map<String, List<DisplayFundingRate>> out = new LinkedHashMap<>();
    for (String symbol : symbols) {
      List<FutureFundingRate> rows = queryFundingRates(symbol, sinceMs, untilMs, limit).stream()
          .filter(rate -> rate.getFundingTime() != null)
          .filter(rate -> sinceMs == null || rate.getFundingTime() >= sinceMs)
          .filter(rate -> untilMs == null || rate.getFundingTime() < untilMs)
          .sorted((left, right) -> left.getFundingTime().compareTo(right.getFundingTime()))
          .toList();
      if (rows.size() > limit) {
        rows = rows.subList(rows.size() - limit, rows.size());
      }
      for (FutureFundingRate row : rows) {
        if (StringUtils.isBlank(row.getSymbol())) {
          row.setSymbol(symbol);
        }
      }
      List<DisplayFundingRate> displayRows = ConvertUtil.convertToDisplayFundingRates(rows);
      for (DisplayFundingRate row : displayRows) {
        if (row.getFundingTime() != null) {
          bulkFundingHistoricalCache.put(new HistoricalFundingKey(symbol, row.getFundingTime()), row);
        }
      }
      out.put(symbol, displayRows);
    }
    return new BulkFundingRateResponse(System.currentTimeMillis(), out);
  }

  private List<String> normalizeFundingSymbols(Collection<String> symbols) {
    List<String> realSymbols = symbols == null || symbols.isEmpty()
        ? querySymbols()
        : symbols.stream()
            .filter(StringUtils::isNotBlank)
            .map(StringUtils::trim)
            .toList();
    if (realSymbols.isEmpty()) {
      return List.of();
    }
    List<String> sorted = new ArrayList<>(realSymbols.stream()
        .filter(Objects::nonNull)
        .distinct()
        .toList());
    Collections.sort(sorted);
    return sorted;
  }

  @Override
  public List<FuturePremiumIndex> queryPremiumIndices() {
    Call<List<FuturePremiumIndex>> indicesCall = binanceFutureClient.getSymbolPremiumIndices();
    List<FuturePremiumIndex> indices = ClientUtil.getResponseBody(indicesCall,
        () -> rateLimitManager.stopAcquire(Constants.BINANCE_FUTURE_KLINES_FETCHER_RATE_LIMITER_NAME, 1000 * 30));
    return indices == null ? List.of() : indices;
  }

  @Override
  public FuturePremiumIndex queryPremiumIndex(String symbol) {
    Call<FuturePremiumIndex> premiumIndexCall = binanceFutureClient.getSymbolPremiumIndex(symbol);
    return ClientUtil.getResponseBody(premiumIndexCall,
        () -> rateLimitManager.stopAcquire(Constants.BINANCE_FUTURE_KLINES_FETCHER_RATE_LIMITER_NAME, 1000 * 30));
  }

  @Override
  public List<String> querySymbols() {
    return queryExchange().getSymbols().stream()
        .filter(symbol -> StringUtils.equals(symbol.getStatus(), VALID_SYMBOL_STATUS))
        .map(BinanceFutureSymbol::getSymbol)
        .collect(Collectors.toList());
  }

  private void refreshServerTimeDelta() {
    Call<BinanceServerTime> serverTimeCall = binanceFutureClient.getServerTime();
    BinanceServerTime serverTime = ClientUtil.getResponseBody(serverTimeCall,
        () -> rateLimitManager.stopAcquire(Constants.BINANCE_FUTURE_KLINES_FETCHER_RATE_LIMITER_NAME, 1000 * 30));
    if (serverTime.getServerTime() != null) {
      long deltaMills = System.currentTimeMillis() - serverTime.getServerTime();
      serverTimeDelta.set(deltaMills);
    }
  }

  private record RecentFundingKey(List<String> symbols, int limit, long fundingBoundaryMs) {
  }

  private record HistoricalFundingKey(String symbol, long fundingTime) {
  }

  private LoadingCache<String, BinanceFutureExchange> buildExchangeCache() {
    return Caffeine.newBuilder()
        .maximumSize(1)
        .expireAfterWrite(Duration.of(10, ChronoUnit.MINUTES))
        .refreshAfterWrite(5, TimeUnit.MINUTES)
        .build(s -> {
          Call<BinanceFutureExchange> exchangeCall = binanceFutureClient.getExchange();
          return ClientUtil.getResponseBody(exchangeCall,
              () -> rateLimitManager.stopAcquire(Constants.BINANCE_FUTURE_KLINES_FETCHER_RATE_LIMITER_NAME, 1000 * 30));
        });
  }

}
