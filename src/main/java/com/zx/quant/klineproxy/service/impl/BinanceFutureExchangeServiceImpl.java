package com.zx.quant.klineproxy.service.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import retrofit2.Call;

/**
 * binance future exchange service impl
 * @author flamhaze5946
 */
@Slf4j
@Service("binanceFutureExchangeService")
public class BinanceFutureExchangeServiceImpl implements FutureExchangeService<BinanceFutureExchange>, InitializingBean {

  private static final String VALID_SYMBOL_STATUS = "TRADING";

  private static final String SERVER_TIME_REFRESHER_GROUP = "futureServerTimeRefresher";

  private static final String FUNDING_RATE_REFRESH_CRON = "0 0 * * * *";

  private static final String FUNDING_RATE_RETRY_CRON = "0 5 * * * *";

  private static final String FUNDING_RATE_REFRESH_ZONE = "UTC";

  private static final int DEFAULT_BULK_FUNDING_LIMIT = 1;

  private static final int MAX_BULK_FUNDING_LIMIT = 100;

  private static final long RECENT_CACHE_BOUNDARY_MS = 60L * 60L * 1000L;

  private static final long LATEST_FUNDING_LOOKBACK_MS = 8L * 60L * 60L * 1000L;

  private static final long FUNDING_CHUNK_CACHE_WINDOW_THRESHOLD_MS = LATEST_FUNDING_LOOKBACK_MS;

  private static final int FUNDING_CHUNK_FETCH_LIMIT = 1000;

  private final ScheduledExecutorService serverTimeRefresher = new ScheduledThreadPoolExecutor(1,
      ThreadFactoryUtil.getNamedThreadFactory(SERVER_TIME_REFRESHER_GROUP));

  private final LoadingCache<String, BinanceFutureExchange> exchangeCache = buildExchangeCache();

  private final Cache<RecentFundingKey, BulkFundingRateResponse> bulkFundingRecentCache = Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofSeconds(60))
      .maximumSize(64)
      .build();

  private final Cache<Long, Map<String, List<DisplayFundingRate>>> historicalChunkCache = Caffeine.newBuilder()
      .maximumSize(720)
      .build();

  private final AtomicLong serverTimeDelta = new AtomicLong(0);

  @Autowired
  private BinanceFutureClient binanceFutureClient;

  @Autowired
  private RateLimitManager rateLimitManager;

  @Value("${funding.publicationGraceMs:50}")
  private long fundingPublicationGraceMs;

  @Override
  public void afterPropertiesSet() throws Exception {
    new ExceptionSafeRunnable(this::refreshServerTimeDelta).run();
    serverTimeRefresher.scheduleAtFixedRate(new ExceptionSafeRunnable(this::refreshServerTimeDelta), 5, 3600, TimeUnit.SECONDS);
    new ExceptionSafeRunnable(this::warmBulkFundingRatesCache).run();
  }

  @Scheduled(cron = FUNDING_RATE_REFRESH_CRON, zone = FUNDING_RATE_REFRESH_ZONE)
  public void warmBulkFundingRatesCache() {
    long now = System.currentTimeMillis();
    long boundary = Math.floorDiv(now, RECENT_CACHE_BOUNDARY_MS) * RECENT_CACHE_BOUNDARY_MS;
    long target = boundary + fundingPublicationGraceMs + 1L;
    long waitMs = target - now;
    if (waitMs > 0) {
      try {
        Thread.sleep(waitMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
    queryBulkFundingRates(null, null, null, DEFAULT_BULK_FUNDING_LIMIT);
  }

  @Scheduled(cron = FUNDING_RATE_RETRY_CRON, zone = FUNDING_RATE_REFRESH_ZONE)
  public void retryBulkFundingRatesCache() {
    long now = System.currentTimeMillis();
    long boundary = Math.floorDiv(now, RECENT_CACHE_BOUNDARY_MS) * RECENT_CACHE_BOUNDARY_MS;
    historicalChunkCache.invalidate(boundary);
    queryBulkFundingRates(null, null, null, DEFAULT_BULK_FUNDING_LIMIT);
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
    rateLimitManager.acquire(Constants.BINANCE_FUTURE_KLINES_FETCHER_RATE_LIMITER_NAME, 5);
    Call<List<FutureFundingRate>> ratesCall = binanceFutureClient.getFundingRates(symbol, startTime, endTime, limit);
    List<FutureFundingRate> rates = ClientUtil.getResponseBody(ratesCall,
        () -> rateLimitManager.stopAcquire(Constants.BINANCE_FUTURE_KLINES_FETCHER_RATE_LIMITER_NAME, 1000 * 30));
    return rates == null ? List.of() : rates;
  }

  @Override
  public BulkFundingRateResponse queryBulkFundingRates(Collection<String> symbols, Long sinceMs, Long untilMs, Integer limit) {
    int realLimit = Math.min(Math.max(limit != null ? limit : DEFAULT_BULK_FUNDING_LIMIT, 1),
        MAX_BULK_FUNDING_LIMIT);
    boolean noSymbolsSpecified = symbols == null || symbols.isEmpty();
    if (sinceMs != null && untilMs != null) {
      if (noSymbolsSpecified) {
        return loadBulkFundingRatesViaChunkCache(null, sinceMs, untilMs, realLimit);
      }
      long windowMs = untilMs - sinceMs;
      if (windowMs > 0 && windowMs <= FUNDING_CHUNK_CACHE_WINDOW_THRESHOLD_MS) {
        return loadBulkFundingRatesViaChunkCache(
            normalizeFundingSymbols(symbols), sinceMs, untilMs, realLimit);
      }
      return loadBulkFundingRates(normalizeFundingSymbols(symbols), sinceMs, untilMs, realLimit);
    }
    if (sinceMs == null && untilMs == null) {
      long now = System.currentTimeMillis();
      long fundingBoundaryMs = Math.floorDiv(now, RECENT_CACHE_BOUNDARY_MS) * RECENT_CACHE_BOUNDARY_MS;
      if (noSymbolsSpecified) {
        boolean withinPublicationGrace = (now - fundingBoundaryMs) < fundingPublicationGraceMs;
        if (withinPublicationGrace) {
          return loadBulkFundingRatesViaChunkCache(null, now - LATEST_FUNDING_LOOKBACK_MS, now, realLimit);
        }
        RecentFundingKey key = new RecentFundingKey(List.of(), realLimit, fundingBoundaryMs);
        return bulkFundingRecentCache.get(key, ignored -> {
          long requestNow = System.currentTimeMillis();
          return loadBulkFundingRatesViaChunkCache(null, requestNow - LATEST_FUNDING_LOOKBACK_MS, requestNow, realLimit);
        });
      }
      List<String> realSymbols = normalizeFundingSymbols(symbols);
      RecentFundingKey key = new RecentFundingKey(realSymbols, realLimit, fundingBoundaryMs);
      return bulkFundingRecentCache.get(key, ignored -> loadBulkFundingRates(realSymbols, null, null, realLimit));
    }
    return loadBulkFundingRates(normalizeFundingSymbols(symbols), sinceMs, untilMs, realLimit);
  }

  /**
   * seed one chunk into the historical chunk cache
   * @author flamhaze5946
   */
  public void seedHistoricalChunk(long chunkStart, Map<String, List<DisplayFundingRate>> chunkData) {
    historicalChunkCache.asMap().putIfAbsent(chunkStart, chunkData);
  }

  private BulkFundingRateResponse loadBulkFundingRatesViaChunkCache(
      List<String> symbols, long sinceMs, long untilMs, int limit) {
    if (sinceMs >= untilMs) {
      return new BulkFundingRateResponse(System.currentTimeMillis(), new LinkedHashMap<>());
    }
    long now = System.currentTimeMillis();
    long firstChunkStart = Math.floorDiv(sinceMs, RECENT_CACHE_BOUNDARY_MS) * RECENT_CACHE_BOUNDARY_MS;
    long lastChunkStart = Math.floorDiv(untilMs - 1, RECENT_CACHE_BOUNDARY_MS) * RECENT_CACHE_BOUNDARY_MS;

    List<Map<String, List<DisplayFundingRate>>> chunksInWindow = new ArrayList<>();
    for (long chunkStart = firstChunkStart; chunkStart <= lastChunkStart; chunkStart += RECENT_CACHE_BOUNDARY_MS) {
      if (chunkStart >= now) {
        break;
      }
      long chunkEnd = chunkStart + RECENT_CACHE_BOUNDARY_MS;
      Map<String, List<DisplayFundingRate>> chunkData;
      if (chunkStart + fundingPublicationGraceMs > now) {
        chunkData = fetchHistoricalChunk(chunkStart, chunkEnd);
      } else {
        final long key = chunkStart;
        chunkData = historicalChunkCache.get(key,
            ck -> fetchHistoricalChunk(ck, ck + RECENT_CACHE_BOUNDARY_MS));
      }
      chunksInWindow.add(chunkData);
    }

    List<String> responseSymbols;
    if (symbols == null || symbols.isEmpty()) {
      responseSymbols = chunksInWindow.stream()
          .flatMap(chunk -> chunk.keySet().stream())
          .distinct()
          .sorted()
          .toList();
    } else {
      responseSymbols = symbols;
    }

    Map<String, List<DisplayFundingRate>> out = new LinkedHashMap<>();
    for (String symbol : responseSymbols) {
      List<DisplayFundingRate> events = new ArrayList<>();
      for (Map<String, List<DisplayFundingRate>> chunk : chunksInWindow) {
        List<DisplayFundingRate> bucket = chunk.get(symbol);
        if (bucket == null) {
          continue;
        }
        for (DisplayFundingRate event : bucket) {
          Long fundingTime = event.getFundingTime();
          if (fundingTime != null && fundingTime >= sinceMs && fundingTime < untilMs) {
            events.add(event);
          }
        }
      }
      events.sort((left, right) -> left.getFundingTime().compareTo(right.getFundingTime()));
      if (events.size() > limit) {
        events = new ArrayList<>(events.subList(events.size() - limit, events.size()));
      }
      out.put(symbol, events);
    }
    return new BulkFundingRateResponse(System.currentTimeMillis(), out);
  }

  private Map<String, List<DisplayFundingRate>> fetchHistoricalChunk(long chunkStart, long chunkEnd) {
    List<FutureFundingRate> rawRows = queryFundingRates(null, chunkStart, chunkEnd, FUNDING_CHUNK_FETCH_LIMIT);
    if (rawRows.size() >= FUNDING_CHUNK_FETCH_LIMIT) {
      log.warn(
          "fetchHistoricalChunk hit FUNDING_CHUNK_FETCH_LIMIT ({}) for chunk [{}, {}); response is likely truncated",
          FUNDING_CHUNK_FETCH_LIMIT, chunkStart, chunkEnd);
    }
    List<FutureFundingRate> rows = rawRows.stream()
        .filter(rate -> rate.getFundingTime() != null)
        .filter(rate -> rate.getSymbol() != null && !rate.getSymbol().isBlank())
        .filter(rate -> rate.getFundingTime() >= chunkStart)
        .filter(rate -> rate.getFundingTime() < chunkEnd)
        .toList();
    Map<String, List<DisplayFundingRate>> chunkData = new LinkedHashMap<>();
    for (FutureFundingRate row : rows) {
      DisplayFundingRate display = ConvertUtil.convertToDisplayFundingRate(row);
      chunkData.computeIfAbsent(row.getSymbol(), key -> new ArrayList<>()).add(display);
    }
    for (List<DisplayFundingRate> bucket : chunkData.values()) {
      bucket.sort((left, right) -> left.getFundingTime().compareTo(right.getFundingTime()));
    }
    return chunkData;
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
