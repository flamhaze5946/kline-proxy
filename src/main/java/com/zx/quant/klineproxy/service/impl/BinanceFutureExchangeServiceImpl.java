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
import com.zx.quant.klineproxy.model.exceptions.ApiException;
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
import org.springframework.http.HttpStatus;
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

  /**
   * Cap on the caller-visible "keep the newest N events per symbol" for a
   * request that carries a time bound. It has to exceed the widest window a
   * caller can ask for divided by the densest funding cadence, or the trim
   * silently drops events the caller needs: the Rust trader passes its
   * `window_hours` here and that reaches `MNOS_A3_MANIFEST_TOLERANCE_HOURS + 4`
   * = 104 live, against the previous cap of 100. The window is the real bound,
   * so a large value costs nothing here.
   */
  private static final int MAX_BULK_FUNDING_LIMIT = 1000;

  /**
   * Cap for a request with no time bound at all. Nothing else limits those, so
   * `limit` alone decides how many rows are fetched AND retained per symbol; at
   * ~526 symbols the ranged cap would permit a half-million-row response and
   * cache entry. This is the value the single cap had before ranged callers
   * needed a higher one.
   */
  private static final int MAX_BULK_FUNDING_LIMIT_NO_RANGE = 100;

  private static final long RECENT_CACHE_BOUNDARY_MS = 60L * 60L * 1000L;

  private static final long CRON_EARLY_FIRE_TOLERANCE_MS = 1000L;

  private static final long LATEST_FUNDING_LOOKBACK_MS = 8L * 60L * 60L * 1000L;

  private static final long FUNDING_CHUNK_CACHE_WINDOW_THRESHOLD_MS = LATEST_FUNDING_LOOKBACK_MS;

  private static final int FUNDING_CHUNK_FETCH_LIMIT = 1000;

  /** Binance-style error code for a funding range that cannot be served in one page. */
  private static final int FUNDING_RANGE_TOO_WIDE_CODE = -1130;

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
    long boundary = resolveScheduledHourBoundary(now);
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
    long boundary = resolveScheduledHourBoundary(System.currentTimeMillis());
    historicalChunkCache.invalidate(boundary);
    int evictedRecent = evictRecentFundingCacheForBoundary(boundary);
    log.info("retryBulkFundingRatesCache invalidated chunk for boundary={} and {} recent entries; refetching",
        boundary, evictedRecent);
    queryBulkFundingRates(null, null, null, DEFAULT_BULK_FUNDING_LIMIT);
  }

  private long resolveScheduledHourBoundary(long now) {
    long floor = Math.floorDiv(now, RECENT_CACHE_BOUNDARY_MS) * RECENT_CACHE_BOUNDARY_MS;
    long ceil = floor + RECENT_CACHE_BOUNDARY_MS;
    return (ceil - now) <= CRON_EARLY_FIRE_TOLERANCE_MS ? ceil : floor;
  }

  private int evictRecentFundingCacheForBoundary(long boundary) {
    int[] evicted = {0};
    bulkFundingRecentCache.asMap().keySet().removeIf(key -> {
      if (key.fundingBoundaryMs() == boundary) {
        evicted[0]++;
        return true;
      }
      return false;
    });
    return evicted[0];
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
    // Two different notions of "bounded", and they must not be conflated: the
    // result ceiling follows what `loadBulkFundingRates` treats as a bounded
    // window (EITHER bound), while chunk assembly needs BOTH bounds to know
    // which hours to walk.
    boolean hasAnyBound = sinceMs != null || untilMs != null;
    int realLimit = Math.min(Math.max(limit != null ? limit : DEFAULT_BULK_FUNDING_LIMIT, 1),
        hasAnyBound ? MAX_BULK_FUNDING_LIMIT : MAX_BULK_FUNDING_LIMIT_NO_RANGE);
    boolean noSymbolsSpecified = symbols == null || symbols.isEmpty();
    if (sinceMs != null && untilMs != null) {
      if (sinceMs >= untilMs) {
        // Degenerate window. Answering centrally keeps it off the per-symbol
        // loader, which would otherwise make one Binance call per symbol to
        // discover that an empty interval is empty.
        return new BulkFundingRateResponse(System.currentTimeMillis(), new LinkedHashMap<>());
      }
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
      // `>` rather than `>=`: a caller landing exactly on the boundary still
      // wants that boundary's funding, and `loadChunk` parks it until Binance
      // has had its publication grace, so the fetch is never premature.
      if (chunkStart > now) {
        break;
      }
      chunksInWindow.add(loadChunk(chunkStart));
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

  /**
   * Load one hour-chunk, honouring Binance's funding publication delay.
   *
   * <p>Callers can land inside the publication grace window: the Rust trader's
   * Lambda children align to the hour boundary and reach this service ~25-40ms
   * after it, before Binance has finished publishing the tick that settled at
   * that boundary. The previous code answered such a caller with an immediate,
   * deliberately uncached {@code fetchHistoricalChunk} — i.e. it raced Binance
   * and returned an empty chunk, which the trader reads as {@code fr = 0} and
   * the backtest does not.
   *
   * <p>Instead we park until the grace window has elapsed and then take the
   * normal cached path. The wait is bounded by {@code publicationGraceMs} and is
   * paid on non-settlement hours too, which is deliberate: the condition is "do
   * not serve a snapshot sampled before publication", not "wait until the chunk
   * is non-empty", so it needs no knowledge of the funding calendar and cannot
   * stall on an hour that was never going to publish.
   *
   * <p>Every caller for a boundary therefore collapses onto one Caffeine load
   * and observes the SAME snapshot. That matters more than freshness here: the
   * 192 children of one trading cycle arrive spread over ~1s, and letting late
   * arrivals see a different funding snapshot than early ones would compute
   * different symbols against different {@code fr} within a single cycle.
   */
  private Map<String, List<DisplayFundingRate>> loadChunk(long chunkStart) {
    Map<String, List<DisplayFundingRate>> cached = historicalChunkCache.getIfPresent(chunkStart);
    if (cached != null) {
      return cached;
    }
    awaitPublicationGrace(chunkStart);
    // The warning belongs INSIDE the loader: all 16 workers can miss the cache,
    // park, and join the same Caffeine load, so warning at the call site would
    // emit up to 16 identical lines for one missed settlement. Reading other
    // keys of this cache from inside a loader is safe — those are lock-free
    // reads, not the mapping updates ConcurrentHashMap forbids.
    return historicalChunkCache.get(chunkStart, ck -> {
      Map<String, List<DisplayFundingRate>> loaded =
          fetchHistoricalChunk(ck, ck + RECENT_CACHE_BOUNDARY_MS);
      warnIfSettlementLooksMissed(ck, loaded);
      return loaded;
    });
  }

  /**
   * Sleep until {@code chunkStart + publicationGraceMs} when we are inside that
   * window. Never sleeps longer than the grace itself, so a historical or future
   * chunk costs nothing. An interrupt aborts the request rather than falling
   * through, because continuing would perform exactly the premature fetch this
   * method exists to prevent — and then cache it.
   */
  private void awaitPublicationGrace(long chunkStart) {
    long waitMs = chunkStart + fundingPublicationGraceMs - System.currentTimeMillis();
    if (waitMs <= 0 || waitMs > fundingPublicationGraceMs) {
      return;
    }
    try {
      Thread.sleep(waitMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "interrupted while awaiting funding publication grace for boundary " + chunkStart, e);
    }
  }

  /**
   * Observation only — no state, no retry, no effect on what is served.
   *
   * <p>If Binance publishes later than the grace, the empty snapshot sticks
   * until the :05 retry cron repairs it, and the trader reads {@code fr = 0} for
   * that cycle. Nothing here tries to compensate: the grace is reported to be
   * sufficient in practice, and every automatic remedy considered was a source
   * of worse failures than the one it covered. This line is how we would find
   * out that assumption was wrong.
   */
  private void warnIfSettlementLooksMissed(
      long chunkStart, Map<String, List<DisplayFundingRate>> chunkData) {
    if (!chunkData.isEmpty()) {
      return;
    }
    for (long cadenceHours : new long[] {8L, 4L}) {
      long step = cadenceHours * RECENT_CACHE_BOUNDARY_MS;
      Map<String, List<DisplayFundingRate>> prev = historicalChunkCache.getIfPresent(chunkStart - step);
      Map<String, List<DisplayFundingRate>> prev2 =
          historicalChunkCache.getIfPresent(chunkStart - 2 * step);
      if (prev != null && !prev.isEmpty() && prev2 != null && !prev2.isEmpty()) {
        log.warn("funding chunk boundary={} is EMPTY but the two preceding {}h boundaries both"
                + " published — Binance was likely slower than the {}ms publication grace."
                + " Traders reading this boundary will see fr=0 until the :05 retry cron.",
            chunkStart, cadenceHours, fundingPublicationGraceMs);
        return;
      }
    }
  }

  /**
   * Upper bound on how many funding events one symbol can have inside a range,
   * assuming the densest cadence Binance runs (hourly). One-sided ranges are
   * unbounded, so they can always exceed a page.
   */
  private static long maxHourlyEventsIn(Long sinceMs, Long untilMs) {
    if (sinceMs == null || untilMs == null || sinceMs >= untilMs) {
      return Long.MAX_VALUE;
    }
    return Math.floorDiv(untilMs - 1, RECENT_CACHE_BOUNDARY_MS)
        - Math.floorDiv(sinceMs, RECENT_CACHE_BOUNDARY_MS) + 1;
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
    // `limit` is OUR "keep the newest N" cap, which is not what Binance does
    // with it: given a startTime/endTime bracketing more rows than the limit,
    // `GET /fapi/v1/fundingRate` returns the EARLIEST N ("response will be sent
    // as startTime + limit"). Forwarding it would drop the NEWEST event — the
    // one the caller's most recent candle needs — and the trim below cannot
    // recover a row that never arrived. So ask for a full page whenever a bound
    // is present and do the newest-N trim here. With no bound Binance already
    // returns the most recent N, so `limit` passes through untouched.
    boolean bounded = sinceMs != null || untilMs != null;
    int upstreamLimit = bounded ? Math.max(limit, FUNDING_CHUNK_FETCH_LIMIT) : limit;
    for (String symbol : symbols) {
      List<FutureFundingRate> rawRows = queryFundingRates(symbol, sinceMs, untilMs, upstreamLimit);
      if (bounded && rawRows.size() >= upstreamLimit
          && maxHourlyEventsIn(sinceMs, untilMs) > upstreamLimit) {
        // The page came back full, so the range MAY hold more than one page —
        // and Binance truncates a bounded range at the NEWEST end, which is
        // exactly the end callers need. The newest-N trim below can only choose
        // from what arrived, so returning this would silently drop the caller's
        // most recent events while reporting success.
        //
        // Fail instead of warning. The guard is two-part because a full page
        // alone is not proof of truncation: a two-sided range that CANNOT hold
        // more than one page is complete however full it comes back, so only a
        // range with room for more is rejected. The Lambda trader is therefore
        // structurally exempt — `MAX_PROXY_WINDOW_HOURS` is 999, one below the
        // page — rather than exempt by argument, which is how the earlier
        // "unreachable" claim was wrong at exactly that boundary.
        //
        // `ApiException` and NOT `ResponseStatusException`: `GlobalExceptionConfig`
        // has an `@ExceptionHandler(Exception.class)` that would flatten the
        // latter to 500, and `is_retryable_proxy_err` in the Rust client retries
        // 5xx three times before giving up. 400 is terminal, which is what a
        // request that will never succeed as posed should be. The in-repo caller
        // that can still reach this converts any proxy error into a soft failure
        // and queries Binance directly (`scraper/runner.rs`: "kline_proxy soft
        // failure, falling back to fapi REST").
        throw new ApiException(HttpStatus.BAD_REQUEST, FUNDING_RANGE_TOO_WIDE_CODE, String.format(
            "funding range [%s, %s) for %s filled the %d-row page; it may span more than one"
                + " page and Binance truncates such a range at the newest end. Narrow the"
                + " window or query Binance directly.",
            sinceMs, untilMs, symbol, upstreamLimit));
      }
      List<FutureFundingRate> rows = rawRows.stream()
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
