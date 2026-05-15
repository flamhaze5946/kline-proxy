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

  // Re-warm the bulk-funding recent cache at the start of every integer
  // hour, +1s buffer (matches FUNDING_PUBLICATION_GRACE_MS) so Binance
  // has finished publishing the new boundary's events. Pinned to UTC
  // because Binance funding boundaries are UTC-aligned.
  private static final String FUNDING_RATE_REFRESH_CRON = "1 0 * * * *";

  private static final String FUNDING_RATE_REFRESH_ZONE = "UTC";

  private static final int DEFAULT_BULK_FUNDING_LIMIT = 1;

  private static final int MAX_BULK_FUNDING_LIMIT = 100;

  // Recent-cache key bucket: aligned to the integer hour so the cache
  // self-invalidates at every funding boundary (Binance supports 1h /
  // 4h / 8h intervals — 1h is the GCD that covers them all). At each
  // hourly mark the key flips and the next request triggers a fresh
  // fetch even if the 60s TTL has not expired yet.
  private static final long RECENT_CACHE_BOUNDARY_MS = 60L * 60L * 1000L;

  // Grace buffer for Binance publication latency. Binance publishes a
  // boundary's funding events within ~1s of the boundary, so a 1s buffer
  // is enough to prevent (a) locking an empty/partial just-finished chunk
  // into historicalChunkCache, and (b) caching an aggregate missing
  // late-publishing events for 60s in bulkFundingRecentCache. Bump only
  // if fetchHistoricalChunk warn logs or scraper anomalies show
  // publication routinely exceeding it.
  private static final long FUNDING_PUBLICATION_GRACE_MS = 1_000L;

  // Lookback for the "latest funding rate per symbol" query (no symbol,
  // no time). Covers Binance's longest supported funding interval (8h) so
  // every active symbol has fired at least once inside the window.
  private static final long LATEST_FUNDING_LOOKBACK_MS = 8L * 60L * 60L * 1000L;

  // Threshold for routing a windowed funding query through the chunk
  // cache vs the per-symbol REST loop. For "short" windows (default
  // 8h, matches LATEST_FUNDING_LOOKBACK_MS), routing N symbols through
  // the chunk cache is cheap: ~1 no-symbol Binance call per 1h chunk
  // (cached), then post-filter by symbol in memory. The per-symbol loop
  // costs weight 5 × N under the 35/sec rate limiter — e.g. 8 shards ×
  // 70 symbols = 560 calls × weight 5 = 2800 weight units = ~80s
  // throttled. For LONG windows (e.g. 30d backfill of one symbol), the
  // per-symbol loop is still cheaper than ~720 cache fetches; keep
  // that branch unchanged for long-window single-symbol callers.
  private static final long FUNDING_CHUNK_CACHE_WINDOW_THRESHOLD_MS =
      LATEST_FUNDING_LOOKBACK_MS;

  // Threshold at which fetchHistoricalChunk warns about a possibly
  // truncated response. A 1h chunk holds <= 1 event per symbol, so a
  // 1000-row response strongly suggests Binance hit its hard cap.
  private static final int FUNDING_CHUNK_FETCH_LIMIT = 1000;

  private final ScheduledExecutorService serverTimeRefresher = new ScheduledThreadPoolExecutor(1,
      ThreadFactoryUtil.getNamedThreadFactory(SERVER_TIME_REFRESHER_GROUP));

  private final LoadingCache<String, BinanceFutureExchange> exchangeCache = buildExchangeCache();

  private final Cache<RecentFundingKey, BulkFundingRateResponse> bulkFundingRecentCache = Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofSeconds(60))
      .maximumSize(64)
      .build();

  // Historical chunk cache: each entry holds one 1h-aligned chunk
  // (chunkStartMs -> symbol -> events in that chunk). Populated lazily
  // by windowed queries via the no-symbol bulk Binance call (one HTTP
  // round-trip per chunk). Past chunks are immutable so no TTL is set —
  // LRU eviction caps memory. maximumSize ~ 720 covers ~30 days of 1h
  // chunks. ~600 symbols * 1 event/chunk worst case fits in 1000-event
  // fetch limit.
  private final Cache<Long, Map<String, List<DisplayFundingRate>>> historicalChunkCache = Caffeine.newBuilder()
      .maximumSize(720)
      .build();

  private final AtomicLong serverTimeDelta = new AtomicLong(0);

  @Autowired
  private BinanceFutureClient binanceFutureClient;

  @Autowired
  private RateLimitManager rateLimitManager;

  @Override
  public void afterPropertiesSet() throws Exception {
    // Wrap both startup fetches so Binance unreachability at boot does
    // not break ApplicationContext init — both methods make blocking
    // Binance calls and either side can fail independently.
    new ExceptionSafeRunnable(this::refreshServerTimeDelta).run();
    serverTimeRefresher.scheduleAtFixedRate(new ExceptionSafeRunnable(this::refreshServerTimeDelta), 5, 3600, TimeUnit.SECONDS);

    // Warm the bulk-funding recent cache on startup so the first request
    // hits the cache. Hourly re-warming is handled by warmBulkFundingRatesCache().
    new ExceptionSafeRunnable(this::warmBulkFundingRatesCache).run();
  }

  @Scheduled(cron = FUNDING_RATE_REFRESH_CRON, zone = FUNDING_RATE_REFRESH_ZONE)
  public void warmBulkFundingRatesCache() {
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
    // Full window:
    //   - no symbols: chunk cache (fetches every symbol present in the
    //     window, one no-symbol Binance call per 1h chunk + cached).
    //   - symbols + short window: route through chunk cache and post-
    //     filter by symbol. For e.g. trader_calculator pulling the
    //     just-closed candle's funding for ~70 symbols, 1 cached
    //     chunk fetch is far cheaper than 70 per-symbol REST calls
    //     under the 35/sec, weight=5 limiter (~10s throttled).
    //   - symbols + long window: per-symbol loop. Avoids paying N
    //     chunk fetches for a single-symbol multi-day historical
    //     backfill (e.g. 30 days = 720 chunks vs 1 per-symbol call).
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
    // No time at all: recent cache path. Cache key includes the hourly
    // boundary so the 60s TTL cannot serve a stale entry across the
    // funding boundary.
    if (sinceMs == null && untilMs == null) {
      long now = System.currentTimeMillis();
      long fundingBoundaryMs = Math.floorDiv(now, RECENT_CACHE_BOUNDARY_MS) * RECENT_CACHE_BOUNDARY_MS;
      if (noSymbolsSpecified) {
        // Binance's no-symbol /fapi/v1/fundingRate without start/endTime
        // returns only the most recent 200 records globally (limit param
        // does not lift this cap). For ~600 active USD-M symbols that
        // means most symbols are missing from the response. Synthesize a
        // window [now - 8h, now] (8h covers Binance's longest funding
        // interval so every symbol has fired at least once) and route
        // through the chunk cache, which paginates by 1h chunk so each
        // call stays under the 1000-row hard cap.
        boolean withinPublicationGrace = (now - fundingBoundaryMs) < FUNDING_PUBLICATION_GRACE_MS;
        if (withinPublicationGrace) {
          // Bypass the 60s aggregate cache while the just-passed boundary
          // may still be publishing — caching an incomplete aggregate
          // here would serve missing-symbol responses for up to 60s.
          // Chunk cache still single-flights the cacheable past chunks;
          // only the in-progress chunk is fetched per request.
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
    // Partial window (only sinceMs or only untilMs set): rare path, fall
    // back to per-symbol loop with the partial bound forwarded to Binance.
    return loadBulkFundingRates(normalizeFundingSymbols(symbols), sinceMs, untilMs, realLimit);
  }

  /**
   * Seed one 1h-aligned chunk into {@link #historicalChunkCache}. Used
   * by external initializers (e.g. {@code BinanceVisionFundingHistoryLoader})
   * to pre-fill historical funding data at startup. Uses
   * {@code putIfAbsent} so a chunk already populated by the lazy loader
   * (which carries markPrice from the Binance API) is preserved over the
   * Vision-derived version (which omits markPrice).
   */
  public void seedHistoricalChunk(long chunkStart, Map<String, List<DisplayFundingRate>> chunkData) {
    historicalChunkCache.asMap().putIfAbsent(chunkStart, chunkData);
  }

  /**
   * Chunk-cache windowed loader. Splits the requested window into 1h
   * chunks aligned to {@link #RECENT_CACHE_BOUNDARY_MS}, fetches each
   * eligible chunk once via the no-symbol bulk Binance call, and caches
   * it in {@link #historicalChunkCache}. Chunks whose opening boundary
   * is within the {@link #FUNDING_PUBLICATION_GRACE_MS} buffer of
   * {@code now} are fetched but never cached — funding events fire AT
   * the boundary and Binance can publish them a beat later, so only
   * post-grace chunks are guaranteed complete. Subsequent queries that
   * overlap already-finalized chunks are pure cache hits.
   *
   * @param symbols caller-requested symbols. {@code null} or empty
   *                means "every symbol that appears in the fetched
   *                chunks" — used by the no-symbol windowed path.
   */
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
      if (chunkStart + FUNDING_PUBLICATION_GRACE_MS > now) {
        // Within grace of this chunk's opening boundary: fetch fresh,
        // never cache. Funding events fire AT chunkStart (the boundary),
        // not throughout the chunk, so once grace passes after chunkStart
        // the chunk's contents are stable and cacheable for the entire
        // hour — caching at chunkEnd would needlessly delay cache fill by
        // a full hour without changing correctness.
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
    // A 1h chunk holds at most one event per symbol (Binance funding
    // intervals are >= 1h), so the per-call 1000-event cap comfortably
    // covers all active USD-M futures symbols.
    List<FutureFundingRate> rawRows = queryFundingRates(null, chunkStart, chunkEnd, FUNDING_CHUNK_FETCH_LIMIT);
    if (rawRows.size() >= FUNDING_CHUNK_FETCH_LIMIT) {
      // Binance's hard cap was hit, so some events for this hour were
      // silently dropped. If this fires we need to paginate the chunk
      // (split by time or by lastFundingTime cursor); for now surface it
      // so the assumption can be re-validated against current symbol
      // counts and funding cadence.
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
