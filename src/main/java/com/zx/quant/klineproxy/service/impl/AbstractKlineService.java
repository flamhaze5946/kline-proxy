package com.zx.quant.klineproxy.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import com.zx.quant.klineproxy.model.KlineDispatchMetadata;
import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.model.BulkKlinesResponse;
import com.zx.quant.klineproxy.model.EventKline;
import com.zx.quant.klineproxy.model.EventKline.BigDecimalEventKline;
import com.zx.quant.klineproxy.model.EventKline.DoubleEventKline;
import com.zx.quant.klineproxy.model.EventKline.FloatEventKline;
import com.zx.quant.klineproxy.model.EventKline.StringEventKline;
import com.zx.quant.klineproxy.model.EventKlineEvent;
import com.zx.quant.klineproxy.model.EventMiniTicker24HrEvent;
import com.zx.quant.klineproxy.model.EventTicker24HrEvent;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Kline.BigDecimalKline;
import com.zx.quant.klineproxy.model.Kline.DoubleKline;
import com.zx.quant.klineproxy.model.Kline.FloatKline;
import com.zx.quant.klineproxy.model.Kline.StringKline;
import com.zx.quant.klineproxy.model.KlineSet;
import com.zx.quant.klineproxy.model.KlineSetKey;
import com.zx.quant.klineproxy.model.KlineUpdateSource;
import com.zx.quant.klineproxy.model.ParsedWebSocketMessage;
import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.model.Ticker.BigDecimalTicker;
import com.zx.quant.klineproxy.model.Ticker24Hr;
import com.zx.quant.klineproxy.model.config.KlineBulkProperties;
import com.zx.quant.klineproxy.model.config.KlinePersistenceProperties;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties.IntervalSyncConfig;
import com.zx.quant.klineproxy.model.enums.IntervalEnum;
import com.zx.quant.klineproxy.model.enums.NumberTypeEnum;
import com.zx.quant.klineproxy.model.exceptions.ApiException;
import com.zx.quant.klineproxy.model.persistence.PersistedKlineRow;
import com.zx.quant.klineproxy.monitor.MonitorManager;
import com.zx.quant.klineproxy.monitor.ClosedBarLatencyRecorder;
import com.zx.quant.klineproxy.monitor.ClosedBarArrivalTracker;
import com.zx.quant.klineproxy.monitor.ClosedBarLatencyRecorder.Trace;
import com.zx.quant.klineproxy.service.KlinePersistenceStore;
import com.zx.quant.klineproxy.service.FinalBarWaitRegistry;
import com.zx.quant.klineproxy.service.KlineService;
import com.zx.quant.klineproxy.service.TickerPriceBook;
import com.zx.quant.klineproxy.service.stream.AbstractBinanceKlineStream;
import com.zx.quant.klineproxy.service.stream.BinanceKlineStream;
import com.zx.quant.klineproxy.service.stream.BinanceKlineHeader;
import com.zx.quant.klineproxy.util.CommonUtil;
import com.zx.quant.klineproxy.util.HourBoundaryGuard;
import com.zx.quant.klineproxy.util.ConvertUtil;
import com.zx.quant.klineproxy.util.ExceptionSafeRunnable;
import com.zx.quant.klineproxy.util.Serializer;
import com.zx.quant.klineproxy.util.ThreadFactoryUtil;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import jakarta.annotation.PreDestroy;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;

/**
 * abstract kline service
 * @author flamhaze5946
 */
@Slf4j
public abstract class AbstractKlineService<T extends WebSocketClient> implements KlineService, InitializingBean {

  private static final String MANAGE_EXECUTOR_GROUP = "kline-manage";
  
  private static final String KLINE_FETCH_EXECUTOR_GROUP = "kline-fetch";

  private static final ExecutorService MANAGE_EXECUTOR = buildManageExecutor();

  private static final ExecutorService KLINE_FETCH_EXECUTOR = buildKlineFetchExecutor();

  protected static final BinanceKlineStream ORDINARY_KLINE_STREAM = new BinanceKlineStream();

  private static final List<AbstractBinanceKlineStream> DEFAULT_KLINE_STREAMS = List.of(ORDINARY_KLINE_STREAM);

  /** frames arrive every 1000 ms: stale after one missed frame plus delivery jitter */
  protected static final long TICKER_STREAM_STALE_MILLIS = 2_000L;

  /**
   * consecutive frames carry contiguous event times (observed jumps under 50 ms);
   * a lost frame opens a jump of about 1000 ms
   */
  private static final long TICKER_STREAM_GAP_MILLIS = 500L;

  /**
   * how far a full REST snapshot trails its request: measured from request send to the newest
   * liquid-symbol time, up to 563 ms on fapi/v2/ticker/price and 942 ms on api/v3/ticker/24hr
   */
  private static final long TICKER_REST_LAG_MILLIS = 1_000L;

  private static final BigDecimal ONE_HUNDRED = BigDecimal.valueOf(100);

  private static final int SYMBOLS_PER_CONNECTION = 150;

  private static final int MAX_RPC_SYNC_WORKERS = 8;

  private static final int RESTORE_WORKERS =
      Math.max(2, Runtime.getRuntime().availableProcessors());

  private static final int MAX_MAKE_UP_WORKERS = 4;

  private static final int KLINE_TRIM_BUFFER = 50;

  private static final long SUBSCRIBE_TOPICS_ADJUST_INTERVAL_MILLS = 30_000L;

  private static final Duration ALL_MARKET_TICKER_CACHE_TTL = Duration.ofMillis(500);

  private static final Duration BULK_KLINES_CACHE_TTL = Duration.ofSeconds(1);

  private static final int DEFAULT_BULK_KLINES_LIMIT = 5;

  private static final int MAX_BULK_KLINES_LIMIT = 100;

  private static final long ALL_MARKET_SNAPSHOT_REFRESH_IDLE_TIMEOUT_MILLS = 5_000L;

  private static final long ALL_MARKET_SNAPSHOT_REFRESH_CHECK_INTERVAL_MILLS = 100L;

  private static final String KLINE_SCHEDULER_GROUP = "symbols-sync";

  private static final ScheduledExecutorService SCHEDULE_EXECUTOR_SERVICE = new ScheduledThreadPoolExecutor(4,
      ThreadFactoryUtil.getNamedThreadFactory(KLINE_SCHEDULER_GROUP));

  protected static final Integer DEFAULT_LIMIT = 500;

  protected static final Integer MIN_LIMIT = 1;

  protected static final Integer MAX_LIMIT = 1000;

  private final AtomicInteger connectionCount = new AtomicInteger(0);

  private final Map<String, Long> symbolOnboardTimeMap = new ConcurrentHashMap<>();

  private final Set<String> extraSubscribeTopics = ConcurrentHashMap.newKeySet();

  private final Cache<String, Ticker24HrEntry> ticker24HrCache = Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofDays(1))
      .build();

  private final Cache<BulkKlinesKey, BulkKlinesResponse> bulkKlinesCache = Caffeine.newBuilder()
      .expireAfterWrite(BULK_KLINES_CACHE_TTL)
      .maximumSize(64)
      .build();
  private final ConcurrentHashMap<BulkKlinesKey, CompletableFuture<BulkKlinesResponse>> bulkKlinesInFlight =
      new ConcurrentHashMap<>();
  private final AtomicLong finalRevision = new AtomicLong();

  private final ClosedBarArrivalTracker closedBarArrivalTracker =
      new ClosedBarArrivalTracker(this::closedBarUniverse);
  private final ClosedBarLatencyRecorder closedBarLatencyRecorder = new ClosedBarLatencyRecorder();

  @Value("${kline.diagnostics.closedBarLatencyEnabled:true}")
  private boolean closedBarLatencyEnabled = true;

  private final TickerPriceBook tickerPriceBook = new TickerPriceBook(
      TICKER_STREAM_GAP_MILLIS, TICKER_REST_LAG_MILLIS);

  /** serializes full REST snapshots of the price book */
  private final Object tickerPriceSyncLock = new Object();

  private final AtomicBoolean syncingTickerPrices = new AtomicBoolean(false);

  private final AtomicLong lastTickerPriceSyncAttemptTime = new AtomicLong(0L);

  @Value("${kline.ticker.restCompensationIntervalMs:60000}")
  private long tickerRestCompensationIntervalMs = 60_000L;

  /** REST ticker/price passthrough, served only while the stream is silent */
  private final AtomicReference<AllMarketSnapshot<Ticker<?>>> provisionalTickerSnapshot =
      new AtomicReference<>(new AllMarketSnapshot<>(List.of(), 0L));

  private final AtomicBoolean refreshingProvisionalTickerSnapshot = new AtomicBoolean(false);

  private final Object provisionalTickerLock = new Object();

  private final AtomicReference<AllMarketSnapshot<Ticker24Hr>> allMarketTicker24HrSnapshot =
      new AtomicReference<>(new AllMarketSnapshot<>(List.of(), 0L));

  private final AtomicBoolean refreshingAllMarketTicker24HrSnapshot = new AtomicBoolean(false);

  private final AtomicLong lastAllMarketTickerAccessTime = new AtomicLong(0L);

  private final AtomicLong lastAllMarketTicker24HrAccessTime = new AtomicLong(0L);

  private final Set<KlineSetKey> dirtyPersistenceKeys = ConcurrentHashMap.newKeySet();

  private static final int PERSISTENCE_DUMP_LOCK_STRIPES = 64;

  /**
   * serializes reconcile / periodic / shutdown dumps and offline cleanup per key (they run
   * on different threads); striped so the registry stays bounded under symbol churn
   */
  private final Object[] persistenceDumpLocks = buildPersistenceDumpLocks();

  private static Object[] buildPersistenceDumpLocks() {
    Object[] locks = new Object[PERSISTENCE_DUMP_LOCK_STRIPES];
    for (int i = 0; i < locks.length; i++) {
      locks[i] = new Object();
    }
    return locks;
  }

  private volatile ExpectedTopicsSnapshot expectedTopicsSnapshot;

  @Autowired
  protected RateLimitManager rateLimitManager;

  @Autowired
  protected MonitorManager monitorManager;

  @Autowired
  protected Serializer serializer;

  @Autowired
  private KlinePersistenceStore klinePersistenceStore;

  @Autowired
  private KlinePersistenceProperties persistenceProperties;

  @Autowired(required = false)
  private KlineBulkProperties bulkProperties;

  private static final KlineBulkProperties DEFAULT_BULK_PROPERTIES = new KlineBulkProperties();

  private final FinalBarWaitRegistry finalBarWaitRegistry = new FinalBarWaitRegistry();

  @Autowired
  private List<T> webSocketClients = new ArrayList<>();

  @Value("${number.type:bigDecimal}")
  private String numberType;

  protected final Map<KlineSetKey, KlineSet> klineSetMap = new ConcurrentHashMap<>();


  /**
   * query kline by rpc, no cache
   * @param symbol    symbol
   * @param interval  interval
   * @param startTime startTime
   * @param endTime   endTime
   * @param limit     limit
   * @return klines
   */
  protected abstract List<Kline> queryKlines0(String symbol, String interval, Long startTime, Long endTime, Integer limit);

  /**
   * query ticker 24 hrs
   * @return ticker 24hrs
   */
  protected abstract List<Ticker24Hr> queryTicker24Hrs0();

  protected abstract String getRateLimiterName();

  protected abstract List<String> getSymbols();

  protected abstract KlineSyncConfigProperties<? extends IntervalSyncConfig> getSyncConfig();

  protected abstract int getMakeUpKlinesLimit();

  protected abstract int getMakeUpKlinesWeight();

  protected abstract int getTicker24HrsWeight();

  protected abstract String getServiceType();

  protected abstract String getPersistenceServiceCode();

  protected long getServerTime() {
    return System.currentTimeMillis();
  }

  /** the all-market stream feeding the price book and the 24hr fallback cache */
  protected AllMarketTickerStream getAllMarketTickerStream() {
    return AllMarketTickerStream.TICKER;
  }

  /** how long the stream may stay silent before ticker/price reads go to REST */
  protected long getTickerStreamStaleMillis() {
    return TICKER_STREAM_STALE_MILLIS;
  }

  /**
   * Whether full price snapshots come from ticker/24hr (lastPrice at closeTime) instead of
   * {@link #queryTickers0()}: a snapshot must date every price, and spot ticker/price carries no time.
   */
  protected boolean isTickerPriceSnapshotFrom24Hr() {
    return false;
  }

  protected Collection<String> getTicker24HrSubscribeTopics() {
    return Set.of(getAllMarketTickerStream().topic());
  }

  protected List<Ticker24Hr> queryTicker24HrsBySymbols(Collection<String> symbols) {
    return Collections.emptyList();
  }

  protected List<Ticker<?>> queryTickers0() {
    return Collections.emptyList();
  }

  protected List<Ticker<?>> queryTickersBySymbols(Collection<String> symbols) {
    return Collections.emptyList();
  }

  /** A symbol without a price (settling, pending listing) is simply absent from the answer. */
  @Override
  public List<Ticker<?>> queryTickers(Collection<String> symbols) {
    return CollectionUtils.isEmpty(symbols) ? queryAllMarketTickerPrices() : queryTickerPrices(symbols);
  }

  /**
   * Stream silent: REST passthrough, unless the stream resumed while it was in flight. Otherwise the
   * book, once a full snapshot has defined the market; a segment the snapshot does not cover yet
   * (frames were lost) is repaired behind the read.
   */
  private List<Ticker<?>> queryAllMarketTickerPrices() {
    lastAllMarketTickerAccessTime.set(System.currentTimeMillis());
    if (!isTickerStreamAlive()) {
      List<Ticker<?>> provisional = queryProvisionalAllMarketTickers();
      if (!isTickerStreamAlive()) {
        return provisional;
      }
    }
    if (!tickerPriceBook.hasFullSnapshot()) {
      syncTickerPricesIfNoSnapshot();
      if (!tickerPriceBook.hasFullSnapshot()) {
        if (isTickerPriceSnapshotFrom24Hr()) {
          // ticker/price carries no time here: while the stream is live it is not an answer
          throw new ApiException(HttpStatus.SERVICE_UNAVAILABLE, -1001,
              "Ticker prices are not ready yet; please retry.");
        }
        return queryProvisionalAllMarketTickers();
      }
    }
    if (!tickerPriceBook.isCovered()) {
      triggerTickerPriceSyncIfStale();
    }
    return tickerPriceBook.all();
  }

  /**
   * Live stream: the book answers. Silent stream: REST ticker/price, unless the stream resumed while
   * it was in flight; a symbol REST answers without a price has none and leaves the book.
   */
  private List<Ticker<?>> queryTickerPrices(Collection<String> symbols) {
    if (isTickerStreamAlive()) {
      return queryLiveTickerPrices(symbols);
    }
    List<String> requested = symbols.stream().distinct().toList();
    long requestTime = getServerTime();
    List<Ticker<?>> restTickers = queryTickersBySymbols(requested);
    if (restTickers == null) {
      restTickers = List.of();
    }
    tickerPriceBook.applySymbols(restTickers);
    Map<String, Ticker<?>> restBySymbol = restTickers.stream()
        .filter(ticker -> ticker.getSymbol() != null && ticker.getPrice() != null)
        .collect(Collectors.toMap(Ticker::getSymbol, Function.identity(), (o, n) -> n));
    // an empty list for several symbols means the market does not support the query, not "no price"
    boolean answered = !restTickers.isEmpty() || requested.size() == 1;
    Set<String> absent = answered
        ? requested.stream().filter(symbol -> !restBySymbol.containsKey(symbol)).collect(Collectors.toSet())
        : Set.of();
    tickerPriceBook.applyAbsent(absent, requestTime);
    if (isTickerStreamAlive()) {
      // a no-price symbol answers only through an update newer than the request that kept it booked
      List<String> priced = symbols.stream()
          .filter(symbol -> !absent.contains(symbol) || tickerPriceBook.contains(symbol)).toList();
      return priced.isEmpty() ? List.of() : queryLiveTickerPrices(priced);
    }
    List<Ticker<?>> result = new ArrayList<>(symbols.size());
    for (String symbol : symbols) {
      Ticker<?> rest = restBySymbol.get(symbol);
      Ticker<?> ticker = rest != null ? newerOfRestAndBook(rest) : tickerPriceBook.get(symbol);
      if (ticker != null) {
        result.add(ticker);
      }
    }
    return result;
  }

  /**
   * The book answers, after waiting for its first snapshot; a symbol it lacks (a new listing, or every
   * symbol while no snapshot could be taken) is looked up with a dated REST call and joins the book.
   */
  private List<Ticker<?>> queryLiveTickerPrices(Collection<String> symbols) {
    if (!tickerPriceBook.hasFullSnapshot()) {
      syncTickerPricesIfNoSnapshot();
    }
    if (tickerPriceBook.hasFullSnapshot()) {
      if (!tickerPriceBook.isCovered()) {
        triggerTickerPriceSyncIfStale();
      }
      List<Ticker<?>> known = tickerPriceBook.get(symbols);
      if (known.size() == symbols.size()) {
        return known;
      }
      List<String> unknownSymbols = symbols.stream()
          .filter(symbol -> !tickerPriceBook.contains(symbol)).distinct().toList();
      tickerPriceBook.applySymbols(queryDatedTickersBySymbols(unknownSymbols));
      return tickerPriceBook.get(symbols);
    }
    tickerPriceBook.applySymbols(queryDatedTickersBySymbols(symbols.stream().distinct().toList()));
    return tickerPriceBook.get(symbols);
  }

  /**
   * A dated REST price loses to a newer book price. An undated one is only served while the stream
   * is silent, so nothing in the book is newer than it.
   */
  private Ticker<?> newerOfRestAndBook(Ticker<?> rest) {
    if (rest.getTime() <= 0L) {
      return rest;
    }
    Ticker<?> booked = tickerPriceBook.get(rest.getSymbol());
    return booked != null && booked.getTime() > rest.getTime() ? booked : rest;
  }

  /** prices dated by their last update: ticker/24hr closeTime where ticker/price carries no time */
  private List<Ticker<?>> queryDatedTickersBySymbols(Collection<String> symbols) {
    if (!isTickerPriceSnapshotFrom24Hr()) {
      List<Ticker<?>> tickers = queryTickersBySymbols(symbols);
      return tickers == null ? List.of() : tickers;
    }
    List<Ticker24Hr> ticker24Hrs = queryTicker24HrsBySymbols(symbols);
    return ticker24Hrs == null ? List.of() : toPriceTickers(ticker24Hrs);
  }

  private boolean isTickerStreamAlive() {
    return tickerPriceBook.isStreamAlive(getServerTime(), getTickerStreamStaleMillis());
  }

  @Override
  public List<Ticker24Hr> queryTicker24hrs(Collection<String> symbols) {
    if (CollectionUtils.isNotEmpty(symbols)) {
      List<Ticker24Hr> realtimeTicker24Hrs = queryTicker24HrsBySymbols(symbols);
      if (CollectionUtils.isNotEmpty(realtimeTicker24Hrs)) {
        realtimeTicker24Hrs.forEach(this::mergeTicker24Hr);
        return realtimeTicker24Hrs;
      }
    }
    if (CollectionUtils.isEmpty(symbols)) {
      List<Ticker24Hr> realtimeTicker24Hrs = queryAllMarketTicker24HrsSnapshot();
      if (CollectionUtils.isNotEmpty(realtimeTicker24Hrs)) {
        return realtimeTicker24Hrs;
      }
      return ticker24HrCache.asMap().values().stream().map(Ticker24HrEntry::ticker).toList();
    }
    return symbols.stream()
        .map(ticker24HrCache::getIfPresent)
        .filter(Objects::nonNull)
        .map(Ticker24HrEntry::ticker)
        .collect(Collectors.toList());
  }

  @Override
  public ImmutablePair<Collection<Kline>, Integer> queryKlines(String symbol, String interval, Long startTime, Long endTime, int limit, boolean makeUp) {
    IntervalEnum intervalEnum = CommonUtil.getEnumByCode(interval, IntervalEnum.class);
    if (intervalEnum == null) {
      throw new ApiException(HttpStatus.BAD_REQUEST, -1120, "Invalid interval.");
    }
    ImmutablePair<Long, Long> realTimePair = calculateRealStartEndTime(startTime, endTime, intervalEnum, limit);
    Long realStartTime = realTimePair.getLeft();
    Long realEndTime = realTimePair.getRight();

    KlineSetKey key = new KlineSetKey(symbol, intervalEnum.code());
    KlineSet klineSet = klineSetMap.computeIfAbsent(key, var -> new KlineSet(key));
    ConcurrentSkipListMap<Long, Kline> klineSetMap = klineSet.getKlineMap();
    NavigableMap<Long, Kline> savedKlineMap;

    if (makeUp) {
      List<ImmutablePair<Long, Long>> makeUpTimeRanges =
          buildMakeUpTimeRanges(symbol, startTime, endTime, intervalEnum, limit, true);

      if (CollectionUtils.isNotEmpty(makeUpTimeRanges)) {
        fetchAndStoreKlines(symbol, interval,
            filterMakeUpTimeRangesByOnboardTime(symbol, makeUpTimeRanges),
            MAX_MAKE_UP_WORKERS, KLINE_FETCH_EXECUTOR);
      }
    }

    if (MapUtils.isEmpty(klineSetMap)) {
      return ImmutablePair.of(Collections.emptyList(), 0);
    }

    savedKlineMap = klineSetMap
        .subMap(realStartTime, true, realEndTime, true);

    int mapSize = getMapSize(savedKlineMap, intervalEnum);
    if (startTime == null && endTime == null && mapSize < limit) {
      long klinesDuration = calculateKlinesDuration(intervalEnum, limit);
      Kline lastKline = klineSetMap.lastEntry().getValue();
      long lastKlineOpenTime = lastKline.getOpenTime();
      long startKlineOpenTime = lastKlineOpenTime - klinesDuration;
      savedKlineMap = klineSetMap
          .subMap(startKlineOpenTime, true, lastKlineOpenTime, true);
      mapSize = getMapSize(savedKlineMap, intervalEnum);
    }

    return ImmutablePair.of(savedKlineMap.values(), mapSize);
  }

  @Override
  public BulkKlinesResponse queryBulkKlines(String interval, Integer limit, boolean closedOnly, Collection<String> symbols) {
    IntervalEnum intervalEnum = CommonUtil.getEnumByCode(interval, IntervalEnum.class);
    if (intervalEnum == null) {
      throw new ApiException(HttpStatus.BAD_REQUEST, -1120, "Invalid interval.");
    }
    int realLimit = Math.min(Math.max(limit != null ? limit : DEFAULT_BULK_KLINES_LIMIT, MIN_LIMIT),
        MAX_BULK_KLINES_LIMIT);
    List<String> normalizedSymbols = normalizeBulkSymbols(symbols, intervalEnum);
    long now = getServerTime();
    long boundary = Math.floorDiv(now, intervalEnum.getMills()) * intervalEnum.getMills();
    // the boundary is part of the key so a pre-boundary response can never be served after it
    BulkKlinesKey cacheKey = new BulkKlinesKey(intervalEnum.code(), realLimit, closedOnly, normalizedSymbols,
        boundary, finalRevision.get());
    BulkKlinesResponse cached = bulkKlinesCache.getIfPresent(cacheKey);
    if (cached != null) {
      return cached;
    }
    // single-flight per key: concurrent identical requests share one wait+build instead of each
    // holding a Tomcat thread through the settling window (Caffeine's loader cannot skip caching
    // a non-final result, hence the explicit in-flight map)
    CompletableFuture<BulkKlinesResponse> flight = new CompletableFuture<>();
    CompletableFuture<BulkKlinesResponse> existing = bulkKlinesInFlight.putIfAbsent(cacheKey, flight);
    if (existing != null) {
      try {
        return existing.join();
      } catch (java.util.concurrent.CompletionException e) {
        // leader/follower parity: surface the leader's own exception type (e.g. ApiException → its
        // mapped HTTP status) instead of the CompletionException wrapper
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException runtime) {
          throw runtime;
        }
        if (cause instanceof Error error) {
          throw error;
        }
        throw e;
      }
    }
    try {
      FinalWaitOutcome wait = awaitJustClosedBarsFinal(intervalEnum, normalizedSymbols, closedOnly, now, boundary);
      BulkKlinesResponse response = buildBulkKlinesResponse(intervalEnum.code(), realLimit, closedOnly,
          normalizedSymbols, wait);
      // never cache a snapshot that still carries non-final just-closed bars
      if (response.finalized()) {
        bulkKlinesCache.put(cacheKey, response);
      }
      flight.complete(response);
      return response;
    } catch (RuntimeException | Error e) {
      flight.completeExceptionally(e);
      throw e;
    } finally {
      bulkKlinesInFlight.remove(cacheKey, flight);
    }
  }

  /**
   * Block (bounded) until every requested TRADING symbol whose just-closed bar exists has received
   * its final update; symbols whose exchange status is no longer TRADING are skipped and reported. The window is measured from the interval boundary in server time; the actual
   * sleeping uses the wall clock so a frozen test clock cannot spin forever.
   */
  private FinalWaitOutcome awaitJustClosedBarsFinal(IntervalEnum intervalEnum, List<String> symbols,
      boolean closedOnly, long now, long boundary) {
    KlineBulkProperties props = getBulkProperties();
    long maxWaitMs = props.effectiveFinalWaitMaxMs();
    long justClosedOpenTime = boundary - intervalEnum.getMills();
    long sinceBoundary = now - boundary;
    if (!closedOnly) {
      // closed_only=false callers get the forming bar too: finality is not their contract and the
      // response must keep being cached exactly as before this feature
      return new FinalWaitOutcome(justClosedOpenTime, List.of(), 0L, List.of());
    }
    // status != TRADING (SETTLING / CLOSE / BREAK …): the closing update may never come, so the
    // symbol is not waited for; it is reported in not_trading instead of blocking until the cap
    Set<String> trading = tradingSymbolsOrNull();
    List<String> notTrading = notTradingSymbolsWithNonFinalBar(intervalEnum.code(), symbols, justClosedOpenTime, trading);
    List<String> pending = pendingSymbols(intervalEnum.code(), symbols, justClosedOpenTime, trading);
    int pendingInitial = pending.size();
    boolean shouldWait = props.isFinalWaitEnabled() && maxWaitMs > 0
        && sinceBoundary >= 0 && sinceBoundary <= maxWaitMs && !pending.isEmpty();
    long waitedMs = 0L;
    if (shouldWait) {
      long budgetNanos = (maxWaitMs - sinceBoundary) * 1_000_000L;
      long startNanos = System.nanoTime();
      List<FinalBarWaitRegistry.Key> keys = pending.stream()
          .map(symbol -> new FinalBarWaitRegistry.Key(symbol, intervalEnum.code(), justClosedOpenTime))
          .toList();
      try (var registration = finalBarWaitRegistry.register(keys)) {
        while (true) {
          long observed = registration.version();
          // Recheck after registering and before sleeping. A close during this check increments
          // the version, so awaitChange returns immediately rather than missing the notification.
          pending = pendingSymbols(intervalEnum.code(), symbols, justClosedOpenTime, trading);
          long remainingNanos = budgetNanos - (System.nanoTime() - startNanos);
          if (pending.isEmpty() || remainingNanos <= 0) {
            break;
          }
          try {
            // Fallback also observes cache removals, which are not final publications.
            registration.awaitChange(observed, Math.min(TimeUnit.MILLISECONDS.toNanos(25), remainingNanos));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
      waitedMs = (System.nanoTime() - startNanos) / 1_000_000L;
      if (pending.isEmpty()) {
        log.info("BULK_FINAL_WAIT interval={} boundary={} requested={} pending_initial={} waited_ms={} pending_after=0 not_trading={}",
            intervalEnum.code(), boundary, symbols.size(), pendingInitial, waitedMs, notTrading);
      } else {
        log.warn("BULK_FINAL_WAIT_CAP interval={} boundary={} requested={} pending_initial={} waited_ms={} pending_after={} pending={} not_trading={}",
            intervalEnum.code(), boundary, symbols.size(), pendingInitial, waitedMs, pending.size(),
            pending.size() <= 20 ? pending : pending.subList(0, 20), notTrading);
      }
    }
    return new FinalWaitOutcome(justClosedOpenTime, pending, waitedMs, notTrading);
  }

  /**
   * symbols whose just-closed bar EXISTS in memory but is not final yet and whose exchange status is
   * TRADING (absent bars and non-trading symbols are not waited for)
   */
  private List<String> pendingSymbols(String interval, List<String> symbols, long justClosedOpenTime,
      Set<String> trading) {
    List<String> pending = new ArrayList<>();
    for (String symbol : symbols) {
      if (isTrading(trading, symbol) && hasNonFinalBar(symbol, interval, justClosedOpenTime)) {
        pending.add(symbol);
      }
    }
    return pending;
  }

  /** requested symbols that still carry a non-final just-closed bar but are no longer TRADING */
  private List<String> notTradingSymbolsWithNonFinalBar(String interval, List<String> symbols,
      long justClosedOpenTime, Set<String> trading) {
    if (trading == null) {
      return List.of();
    }
    List<String> notTrading = new ArrayList<>();
    for (String symbol : symbols) {
      if (!trading.contains(symbol) && hasNonFinalBar(symbol, interval, justClosedOpenTime)) {
        notTrading.add(symbol);
      }
    }
    return notTrading;
  }

  private boolean hasNonFinalBar(String symbol, String interval, long openTime) {
    KlineSet klineSet = klineSetMap.get(new KlineSetKey(symbol, interval));
    return klineSet != null && klineSet.getKlineMap().containsKey(openTime) && !klineSet.isFinal(openTime);
  }

  /**
   * Current TRADING symbols from exchange info, or {@code null} when unknown (exchange info
   * unavailable or empty): callers then fall back to treating every symbol as trading, i.e. the
   * pre-1.7.16 behaviour of waiting for all of them.
   */
  private Set<String> tradingSymbolsOrNull() {
    try {
      List<String> symbols = getSymbols();
      return CollectionUtils.isEmpty(symbols) ? null : new HashSet<>(symbols);
    } catch (RuntimeException e) {
      log.warn("tradingSymbolsOrNull: exchange info unavailable, not filtering by symbol status: {}", e.toString());
      return null;
    }
  }

  private static boolean isTrading(Set<String> trading, String symbol) {
    return trading == null || trading.contains(symbol);
  }

  private BulkKlinesResponse buildBulkKlinesResponse(String interval, int limit, boolean closedOnly,
      List<String> symbols, FinalWaitOutcome wait) {
    long now = getServerTime();
    Map<String, List<Object[]>> out = new LinkedHashMap<>();
    for (String symbol : symbols) {
      KlineSet klineSet = klineSetMap.get(new KlineSetKey(symbol, interval));
      if (klineSet == null || MapUtils.isEmpty(klineSet.getKlineMap())) {
        continue;
      }
      List<Kline> klines = klineSet.getKlineMap()
          .descendingMap()
          .values()
          .stream()
          .filter(kline -> !closedOnly || kline.getCloseTime() <= now)
          .limit(limit)
          .collect(Collectors.toCollection(ArrayList::new));
      if (klines.isEmpty()) {
        continue;
      }
      Collections.reverse(klines);
      out.put(symbol, klines.stream().map(ConvertUtil::convertToDisplayKline).toList());
    }
    List<String> pending = List.copyOf(wait.pending());
    return new BulkKlinesResponse(interval, now, out, pending.isEmpty(), pending, wait.waitedMs(),
        List.copyOf(wait.notTrading()));
  }

  private record FinalWaitOutcome(long justClosedOpenTime, List<String> pending, long waitedMs,
      List<String> notTrading) {
  }

  private List<String> normalizeBulkSymbols(Collection<String> symbols, IntervalEnum intervalEnum) {
    if (CollectionUtils.isEmpty(symbols)) {
      return klineSetMap.keySet().stream()
          .filter(key -> StringUtils.equals(key.getInterval(), intervalEnum.code()))
          .map(KlineSetKey::getSymbol)
          .distinct()
          .sorted()
          .toList();
    }
    return symbols.stream()
        .filter(StringUtils::isNotBlank)
        .map(StringUtils::trim)
        .filter(symbol -> klineSetMap.containsKey(new KlineSetKey(symbol, intervalEnum.code())))
        .distinct()
        .sorted()
        .toList();
  }

  private record BulkKlinesKey(String interval, int limit, boolean closedOnly, List<String> symbols,
      long boundary, long finalRevision) {
  }

  /**
   * REST callers (full sync, make-up, external callers): a REST candle whose close time has
   * passed is final by definition, so it is marked final here.
   */
  @Override
  public void updateKlines(String symbol, String interval, List<Kline> klines) {
    updateKlinesInternal(symbol, interval, klines, true);
  }

  /**
   * Websocket path: the bar is final only when Binance says so ({@code x=true}). A pre-close
   * update must never mark the bar final — that is exactly the snapshot bulk callers must not
   * receive as a closed bar.
   */
  protected void updateStreamKline(String symbol, String interval, Kline kline, boolean closed) {
    updateStreamKline(symbol, interval, kline, closed, null);
  }

  /**
   * @param eventTimeMs Binance's own event timestamp {@code E}, or null when the caller has none.
   *     E is not a guaranteed socket send time. Transport diagnostics distinguish our frame
   *     callback, executor queue and processing; the legacy settle offset is sampled later.
   */
  protected void updateStreamKline(String symbol, String interval, Kline kline, boolean closed,
      Long eventTimeMs) {
    updateStreamKline(symbol, interval, kline, closed, eventTimeMs, null);
  }

  private void updateStreamKline(String symbol, String interval, Kline kline, boolean closed,
      Long eventTimeMs, Trace trace) {
    updateStreamKline(symbol, interval, kline, closed, eventTimeMs, trace, 0L);
  }

  private void updateStreamKline(String symbol, String interval, Kline kline, boolean closed,
      Long eventTimeMs, Trace trace, long receiveSequence) {
    if (kline == null) {
      return;
    }
    if (trace != null) {
      trace.cacheStarting();
    }
    KlineSet.Commit commit = updateStreamKlineCache(symbol, interval, kline, closed, eventTimeMs, receiveSequence);
    if (trace != null) {
      trace.cacheUpdated();
    }
    if (commit.becameFinal()) {
      signalFinal(symbol, interval, kline.getOpenTime());
    }
    if (closed) {
      if (trace != null) {
        trace.finalized();
      }
      recordClosedBarArrival(symbol, interval, kline.getOpenTime(), eventTimeMs);
      if (trace != null) {
        trace.settleRecorded();
      }
    }
  }

  /** A single current/adjacent/existing bar needs no batch lists, sorting or gap-fill bookkeeping. */
  private KlineSet.Commit updateStreamKlineCache(String symbol, String interval, Kline kline,
      boolean closed, Long eventTimeMs, long receiveSequence) {
    IntervalEnum intervalEnum = CommonUtil.getEnumByCode(interval, IntervalEnum.class);
    if (intervalEnum == null || StringUtils.isBlank(symbol)) {
      return KlineSet.Commit.UNCHANGED;
    }
    KlineSetKey key = new KlineSetKey(symbol, interval);
    KlineSet set = klineSetMap.computeIfAbsent(key, KlineSet::new);
    KlineSet.Commit commit = null;
    synchronized (set) {
      Entry<Long, Kline> last = set.getKlineMap().lastEntry();
      long open = kline.getOpenTime();
      if (last == null || open == last.getKey()
          || open > last.getKey() && open - last.getKey() == intervalEnum.getMills()
          || open < last.getKey() && set.getKlineMap().containsKey(open)) {
        commit = set.commit(kline, closed, KlineUpdateSource.STREAM, eventTimeMs, receiveSequence);
      }
    }
    if (commit == null) {
      return updateKlinesInternal(symbol, interval, Collections.singletonList(kline), false,
          closed, eventTimeMs, receiveSequence);
    }
    afterKlineCommit(set, intervalEnum, commit);
    return commit;
  }

  /**
   * Answer "how long after the boundary is the previous bar available for every symbol":
   * record the arrival (ms after the boundary) of each symbol's closing update, log
   * {@code CLOSED_BAR_SETTLED} once every symbol that has a bar for that open time is final, or
   * {@code CLOSED_BAR_SETTLE_INCOMPLETE} when some are still missing 30 s after the boundary.
   */
  private void recordClosedBarArrival(String symbol, String interval, long openTime,
      Long eventTimeMs) {
    IntervalEnum intervalEnum = CommonUtil.getEnumByCode(interval, IntervalEnum.class);
    if (intervalEnum != null) {
      long boundary = openTime + intervalEnum.getMills();
      closedBarArrivalTracker.record(getServiceType(), symbol, interval, openTime, intervalEnum.getMills(),
          getServerTime() - boundary, eventTimeMs);
    }
  }

  private ClosedBarArrivalTracker.Universe closedBarUniverse(String interval, long openTime) {
    Set<String> trading = tradingSymbolsOrNull();
    Set<String> withBar = new HashSet<>();
    for (Map.Entry<KlineSetKey, KlineSet> entry : klineSetMap.entrySet()) {
      if (StringUtils.equals(entry.getKey().getInterval(), interval)
          && entry.getValue().getKlineMap().containsKey(openTime)) {
        withBar.add(entry.getKey().getSymbol());
      }
    }
    return new ClosedBarArrivalTracker.Universe(trading, withBar);
  }

  /** Scheduled reconciliation covers status changes while a boundary remains incomplete. */
  void logIncompleteSettles() {
    long now = getServerTime();
    closedBarLatencyRecorder.logDue(getServiceType(), now);
    closedBarArrivalTracker.maintain(getServiceType(), now);
  }

  /** Compatibility view of the most recent completed/incomplete boundary summary. */
  public ClosedBarSettleSummary getLastClosedBarSettle(String interval) {
    ClosedBarArrivalTracker.Summary summary = closedBarArrivalTracker.lastSummary(interval);
    return summary == null ? null : new ClosedBarSettleSummary(summary.interval(), summary.boundary(),
        summary.expected(), summary.arrived(), summary.firstMs(), summary.p50Ms(), summary.p90Ms(),
        summary.maxMs(), summary.lastSymbol(), summary.incomplete(), summary.notTrading().size());
  }

  public record ClosedBarSettleSummary(String interval, long boundary, int expected, int arrived,
      long firstMs, long p50Ms, long p90Ms, long maxMs, String lastSymbol, boolean incomplete,
      /** symbols holding the bar whose exchange status is not TRADING (excluded from expected) */
      int notTrading) {
  }

  private void updateKlinesInternal(String symbol, String interval, List<Kline> klines,
      boolean finalizeClosed) {
    updateKlinesInternal(symbol, interval, klines, finalizeClosed, false, null, 0L);
  }

  private KlineSet.Commit updateKlinesInternal(String symbol, String interval, List<Kline> klines,
      boolean finalizeClosed, boolean streamClosed, Long eventTimeMs, long receiveSequence) {
    if (CollectionUtils.isEmpty(klines) || StringUtils.isBlank(symbol) || StringUtils.isBlank(interval)) {
      return KlineSet.Commit.UNCHANGED;
    }
    IntervalEnum intervalEnum = CommonUtil.getEnumByCode(interval, IntervalEnum.class);
    KlineSetKey klineSetKey = new KlineSetKey(symbol, interval);
    KlineSet klineSet = klineSetMap.computeIfAbsent(klineSetKey, var -> new KlineSet(klineSetKey));
    NavigableMap<Long, Kline> klineMap = klineSet.getKlineMap();
    Entry<Long, Kline> existLastEntry = klineMap.lastEntry();
    Set<Long> inputOpenTimes = klines.stream().map(Kline::getOpenTime).collect(Collectors.toSet());
    List<Kline> klinesToUpdate = new ArrayList<>();
    if (existLastEntry != null && !inputOpenTimes.contains(existLastEntry.getKey())) {
      klinesToUpdate.add(existLastEntry.getValue());
    }
    klinesToUpdate.addAll(klines);
    klinesToUpdate = fillKlines(klinesToUpdate, intervalEnum);
    boolean updated = false;
    boolean anyFinal = false;
    boolean finalRevised = false;
    long now = getServerTime();
    for (Kline kline : klinesToUpdate) {
      boolean input = inputOpenTimes.contains(kline.getOpenTime());
      boolean synthetic = !input
          && (existLastEntry == null || kline.getOpenTime() != existLastEntry.getKey());
      if (!input && !synthetic) {
        continue; // retained last bar is only a gap-fill anchor, not a fresh REST observation
      }
      // A WS gap is only a placeholder: the delayed real close can still be in flight.
      boolean closed = synthetic ? finalizeClosed && kline.getCloseTime() <= now
          : input && (streamClosed || finalizeClosed && kline.getCloseTime() <= now);
      KlineUpdateSource source = synthetic ? KlineUpdateSource.SYNTHETIC
          : finalizeClosed ? KlineUpdateSource.REST : KlineUpdateSource.STREAM;
      KlineSet.Commit commit = klineSet.commit(kline, closed, source, eventTimeMs, receiveSequence);
      if (finalizeClosed && commit.becameFinal()) {
        signalFinal(symbol, interval, kline.getOpenTime());
      }
      updated |= commit.updated();
      anyFinal |= commit.becameFinal();
      finalRevised |= commit.finalRevised();
    }
    KlineSet.Commit result = new KlineSet.Commit(updated, anyFinal, finalRevised);
    afterKlineCommit(klineSet, intervalEnum, result);
    return result;
  }

  private void afterKlineCommit(KlineSet klineSet, IntervalEnum intervalEnum, KlineSet.Commit commit) {
    trimKlinesIfNeeded(klineSet, intervalEnum);
    if ((commit.updated() || commit.becameFinal()) && isPersistenceEnabledFor(intervalEnum)) {
      dirtyPersistenceKeys.add(klineSet.getKey());
    }
    if (commit.finalRevised()) {
      // Rare final corrections must not keep serving a previously cached closed snapshot.
      finalRevision.incrementAndGet();
      bulkKlinesCache.invalidateAll();
    }
  }

  private void signalFinal(String symbol, String interval, long openTime) {
    finalBarWaitRegistry.signal(symbol, interval, openTime);
  }

  protected KlineBulkProperties getBulkProperties() {
    return bulkProperties != null ? bulkProperties : DEFAULT_BULK_PROPERTIES;
  }

  private List<Kline> fillKlines(List<Kline> klines, IntervalEnum intervalEnum) {
    if (CollectionUtils.isEmpty(klines) || klines.size() == 1) {
      return klines;
    }
    List<Kline> filledKlines = new ArrayList<>(klines.size());
    klines.sort(Comparator.comparingLong(Kline::getOpenTime));
    Kline previousKline = null;
    for (Kline currentKline : klines) {
      if (previousKline == null) {
        filledKlines.add(currentKline);
        previousKline = currentKline;
        continue;
      }

      long expectOpenTime = previousKline.getOpenTime() + intervalEnum.getMills();
      while (expectOpenTime < currentKline.getOpenTime()) {
        Kline insertKline = createFilledKline(previousKline, expectOpenTime, intervalEnum);
        filledKlines.add(insertKline);
        previousKline = insertKline;
        expectOpenTime += intervalEnum.getMills();
      }
      filledKlines.add(currentKline);
      previousKline = currentKline;
    }
    return filledKlines;
  }

  private Kline createFilledKline(Kline previousKline, long openTime, IntervalEnum intervalEnum) {
    long closeTime = openTime + intervalEnum.getMills() - 1;
    if (previousKline instanceof StringKline stringKline) {
      StringKline insertKline = stringKline.deepCopy();
      insertKline.setOpenTime(openTime);
      insertKline.setCloseTime(closeTime);
      insertKline.setHighPrice(stringKline.getClosePrice());
      insertKline.setLowPrice(stringKline.getClosePrice());
      insertKline.setOpenPrice(stringKline.getClosePrice());
      insertKline.setClosePrice(stringKline.getClosePrice());
      insertKline.setVolume("0");
      insertKline.setQuoteVolume("0");
      insertKline.setTradeNum(0);
      insertKline.setActiveBuyVolume("0");
      insertKline.setActiveBuyQuoteVolume("0");
      return insertKline;
    } else if (previousKline instanceof FloatKline floatKline) {
      FloatKline insertKline = floatKline.deepCopy();
      insertKline.setOpenTime(openTime);
      insertKline.setCloseTime(closeTime);
      insertKline.setHighPrice(floatKline.getClosePrice());
      insertKline.setLowPrice(floatKline.getClosePrice());
      insertKline.setOpenPrice(floatKline.getClosePrice());
      insertKline.setClosePrice(floatKline.getClosePrice());
      insertKline.setVolume(0);
      insertKline.setQuoteVolume(0);
      insertKline.setTradeNum(0);
      insertKline.setActiveBuyVolume(0);
      insertKline.setActiveBuyQuoteVolume(0);
      return insertKline;
    } else if (previousKline instanceof DoubleKline doubleKline) {
      DoubleKline insertKline = doubleKline.deepCopy();
      insertKline.setOpenTime(openTime);
      insertKline.setCloseTime(closeTime);
      insertKline.setHighPrice(doubleKline.getClosePrice());
      insertKline.setLowPrice(doubleKline.getClosePrice());
      insertKline.setOpenPrice(doubleKline.getClosePrice());
      insertKline.setClosePrice(doubleKline.getClosePrice());
      insertKline.setVolume(0);
      insertKline.setQuoteVolume(0);
      insertKline.setTradeNum(0);
      insertKline.setActiveBuyVolume(0);
      insertKline.setActiveBuyQuoteVolume(0);
      return insertKline;
    } else if (previousKline instanceof BigDecimalKline bigDecimalKline) {
      BigDecimalKline insertKline = bigDecimalKline.deepCopy();
      insertKline.setOpenTime(openTime);
      insertKline.setCloseTime(closeTime);
      insertKline.setHighPrice(bigDecimalKline.getClosePrice());
      insertKline.setLowPrice(bigDecimalKline.getClosePrice());
      insertKline.setOpenPrice(bigDecimalKline.getClosePrice());
      insertKline.setClosePrice(bigDecimalKline.getClosePrice());
      insertKline.setVolume(BigDecimal.ZERO);
      insertKline.setQuoteVolume(BigDecimal.ZERO);
      insertKline.setTradeNum(0);
      insertKline.setActiveBuyVolume(BigDecimal.ZERO);
      insertKline.setActiveBuyQuoteVolume(BigDecimal.ZERO);
      return insertKline;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    start();
  }

  @PreDestroy
  public void dumpPersistedKlinesOnShutdown() {
    if (!isPersistenceEnabled() || !persistenceProperties.isDumpOnShutdown()) {
      return;
    }
    dumpPersistedKlines(true);
  }

  protected List<IntervalEnum> getSubscribeIntervals() {
    return getSyncConfig().getIntervalSyncConfigs().keySet().stream()
        .map(interval -> CommonUtil.getEnumByCode(interval, IntervalEnum.class))
        .toList();
  }

  protected List<Pattern> getSubscribeSymbolPatterns(IntervalEnum intervalEnum) {
    return getSyncConfig().getIntervalSyncConfigs().get(intervalEnum.code()).getListenSymbolPatterns().stream()
        .map(Pattern::compile)
        .toList();
  }

  protected Function<ParsedWebSocketMessage, Boolean> getKlineEventMessageHandler() {
    return parsedMessage -> {
      AbstractBinanceKlineStream stream = getKlineStreamForEvent(parsedMessage.eventType());
      if (stream == null) {
        return false;
      }
      Trace trace = closedBarLatencyEnabled && parsedMessage.timing() != null
          && parsedMessage.payloadObject() && parsedMessage.payloadNode().path("k").path("x").asBoolean(false)
          ? new Trace(parsedMessage.timing()) : null;
      return doForKlineEvent(stream.parse(parsedMessage, getNumberType(), serializer), trace,
          parsedMessage.receiveSequence());
    };
  }

  protected Function<String, KlineDispatchMetadata> getKlineMessageClassifier() {
    return raw -> {
      BinanceKlineHeader header = BinanceKlineHeader.parse(raw, serializer);
      if (header == null || CommonUtil.getEnumByCode(header.interval(), IntervalEnum.class) == null) {
        return null;
      }
      AbstractBinanceKlineStream stream = getKlineStreamForEvent(header.eventType());
      return stream == null ? null : stream.dispatchMetadata(getServiceType(), header);
    };
  }

  protected List<AbstractBinanceKlineStream> getKlineStreams() {
    return DEFAULT_KLINE_STREAMS;
  }

  protected AbstractBinanceKlineStream getKlineStreamForInterval(String interval) {
    return ORDINARY_KLINE_STREAM;
  }

  private AbstractBinanceKlineStream getKlineStreamForEvent(String eventType) {
    for (AbstractBinanceKlineStream stream : getKlineStreams()) {
      if (stream.accepts(eventType)) {
        return stream;
      }
    }
    return null;
  }

  protected Function<ParsedWebSocketMessage, Boolean> getTicker24HrEventMessageHandler() {
    return parsedMessage -> {
      if (StringUtils.equals(parsedMessage.eventType(), AllMarketTickerStream.TICKER.eventType())) {
        return doForTicker24HrEvents(convertToTickerEvents(parsedMessage, EventTicker24HrEvent.class));
      }
      if (StringUtils.equals(parsedMessage.eventType(), AllMarketTickerStream.MINI_TICKER.eventType())) {
        return doForMiniTicker24HrEvents(convertToTickerEvents(parsedMessage, EventMiniTicker24HrEvent.class));
      }
      return false;
    };
  }

  protected boolean doForKlineEvent(EventKlineEvent<?, ?> eventKlineEvent) {
    return doForKlineEvent(eventKlineEvent, null);
  }

  private boolean doForKlineEvent(EventKlineEvent<?, ?> eventKlineEvent, Trace trace) {
    return doForKlineEvent(eventKlineEvent, trace, 0L);
  }

  private boolean doForKlineEvent(EventKlineEvent<?, ?> eventKlineEvent, Trace trace, long receiveSequence) {
    if (eventKlineEvent == null || getKlineStreamForEvent(eventKlineEvent.getEventType()) == null
        || eventKlineEvent.getEventKline() == null) {
      return false;
    }
    String symbol = eventKlineEvent.getSymbol();
    String interval = eventKlineEvent.getEventKline().getInterval();
    if (StringUtils.isBlank(symbol) || CommonUtil.getEnumByCode(interval, IntervalEnum.class) == null) {
      return false;
    }
    Kline kline = convertToKline(eventKlineEvent);
    Long eventTimeMs = null;
    try {
      String rawEventTime = eventKlineEvent.getEventTime();
      if (StringUtils.isNotBlank(rawEventTime)) {
        eventTimeMs = Long.parseLong(rawEventTime.trim());
      }
    } catch (NumberFormatException ignored) {
      // diagnostics only: a malformed E must never drop a kline update
    }
    updateStreamKline(symbol, interval, kline, eventKlineEvent.getEventKline().isClosed(), eventTimeMs, trace,
        receiveSequence);
    monitorManager.incReceivedKlineMessage(getServiceType(), interval, symbol);
    if (trace != null) {
      trace.completed();
      long clockBeforeMs = System.currentTimeMillis();
      long serverTimeMs = getServerTime();
      long clockAfterMs = System.currentTimeMillis();
      long clockSampleSpanMs = clockAfterMs - clockBeforeMs;
      long clockOffsetMs = serverTimeMs - clockBeforeMs - clockSampleSpanMs / 2;
      long boundary = kline.getOpenTime() + CommonUtil.getEnumByCode(interval, IntervalEnum.class).getMills();
      closedBarLatencyRecorder.record(interval, boundary, new ClosedBarLatencyRecorder.Sample(
          symbol, eventKlineEvent.getEventType(), eventTimeMs, clockOffsetMs, clockSampleSpanMs, trace));
    }
    return true;
  }

  protected boolean doForTicker24HrEvents(List<EventTicker24HrEvent> eventTicker24HrEvents) {
    if (CollectionUtils.isEmpty(eventTicker24HrEvents)) {
      return false;
    }
    Optional<EventTicker24HrEvent> invalidEventOptional = eventTicker24HrEvents.stream()
        .filter(event -> !StringUtils.equals(event.getEventType(), AllMarketTickerStream.TICKER.eventType()))
        .findAny();
    if (invalidEventOptional.isPresent()) {
      return false;
    }
    onTickerStreamFrame(eventTicker24HrEvents.stream().map(EventTicker24HrEvent::getEventTime).toList());
    eventTicker24HrEvents.forEach(this::doForTicker24HrEvent);
    return true;
  }

  protected void doForTicker24HrEvent(EventTicker24HrEvent eventTicker24HrEvent) {
    monitorManager.incReceivedTickerMessage(getServiceType());
    if (eventTicker24HrEvent.getEventTime() != null) {
      tickerPriceBook.updateFromStream(eventTicker24HrEvent.getSymbol(), eventTicker24HrEvent.getLastPrice(),
          eventTicker24HrEvent.getEventTime());
    }
    // the futures market stream also carries coin-margined contracts: only symbols of this market count
    if (!tickerPriceBook.contains(eventTicker24HrEvent.getSymbol())) {
      return;
    }
    Ticker24Hr ticker24Hr = new Ticker24Hr();
    ticker24Hr.setSymbol(eventTicker24HrEvent.getSymbol());
    ticker24Hr.setPriceChange(eventTicker24HrEvent.getPriceChange());
    ticker24Hr.setPriceChangePercent(eventTicker24HrEvent.getPriceChangePercent());
    ticker24Hr.setWeightedAvgPrice(eventTicker24HrEvent.getWeightedAvgPrice());
    ticker24Hr.setPrevClosePrice(eventTicker24HrEvent.getPrevClosePrice());
    ticker24Hr.setLastPrice(eventTicker24HrEvent.getLastPrice());
    ticker24Hr.setLastQty(eventTicker24HrEvent.getLastQty());
    ticker24Hr.setBidPrice(eventTicker24HrEvent.getBidPrice());
    ticker24Hr.setBidQty(eventTicker24HrEvent.getBidQty());
    ticker24Hr.setAskPrice(eventTicker24HrEvent.getAskPrice());
    ticker24Hr.setAskQty(eventTicker24HrEvent.getAskQty());
    ticker24Hr.setOpenPrice(eventTicker24HrEvent.getOpenPrice());
    ticker24Hr.setHighPrice(eventTicker24HrEvent.getHighPrice());
    ticker24Hr.setLowPrice(eventTicker24HrEvent.getLowPrice());
    ticker24Hr.setVolume(eventTicker24HrEvent.getVolume());
    ticker24Hr.setQuoteVolume(eventTicker24HrEvent.getQuoteVolume());
    ticker24Hr.setOpenTime(eventTicker24HrEvent.getOpenTime());
    ticker24Hr.setCloseTime(eventTicker24HrEvent.getCloseTime());
    ticker24Hr.setFirstId(eventTicker24HrEvent.getFirstId());
    ticker24Hr.setLastId(eventTicker24HrEvent.getLastId());
    ticker24Hr.setCount(eventTicker24HrEvent.getCount());

    mergeTicker24Hr(ticker24Hr);
  }

  protected boolean doForMiniTicker24HrEvents(List<EventMiniTicker24HrEvent> events) {
    if (CollectionUtils.isEmpty(events)) {
      return false;
    }
    boolean invalid = events.stream()
        .anyMatch(event -> !StringUtils.equals(event.getEventType(), AllMarketTickerStream.MINI_TICKER.eventType()));
    if (invalid) {
      return false;
    }
    onTickerStreamFrame(events.stream().map(EventMiniTicker24HrEvent::getEventTime).toList());
    events.forEach(this::doForMiniTicker24HrEvent);
    return true;
  }

  protected void doForMiniTicker24HrEvent(EventMiniTicker24HrEvent event) {
    monitorManager.incReceivedTickerMessage(getServiceType());
    if (event.getEventTime() != null) {
      tickerPriceBook.updateFromStream(event.getSymbol(), event.getLastPrice(), event.getEventTime());
    }
    if (tickerPriceBook.contains(event.getSymbol())) {
      mergeMiniTicker24Hr(event);
    }
  }

  /** Runs before the frame's prices land, so a frame after lost ones marks the book uncovered first. */
  private void onTickerStreamFrame(List<Long> eventTimes) {
    long minEventTime = Long.MAX_VALUE;
    long maxEventTime = Long.MIN_VALUE;
    for (Long eventTime : eventTimes) {
      if (eventTime != null) {
        minEventTime = Math.min(minEventTime, eventTime);
        maxEventTime = Math.max(maxEventTime, eventTime);
      }
    }
    if (maxEventTime == Long.MIN_VALUE) {
      return;
    }
    if (tickerPriceBook.onStreamFrame(minEventTime, maxEventTime)) {
      // changes made while frames were lost are only recoverable from a full snapshot
      triggerTickerPriceSync();
    }
  }

  /**
   * A full ticker (REST or stream) updates each field group it is newer for. Its closeTime is the
   * symbol's last update time; an undated one (legacy callers) always applies.
   */
  private void mergeTicker24Hr(Ticker24Hr ticker24Hr) {
    if (ticker24Hr == null || ticker24Hr.getSymbol() == null) {
      return;
    }
    boolean undated = ticker24Hr.getCloseTime() == null;
    long time = undated ? 0L : ticker24Hr.getCloseTime();
    ticker24HrCache.asMap().compute(ticker24Hr.getSymbol(), (symbol, current) -> {
      if (current == null) {
        return new Ticker24HrEntry(ticker24Hr, time, time);
      }
      boolean takeStats = undated || time >= current.statsTime();
      boolean takeFull = undated || time >= current.fullTime();
      if (takeStats && takeFull) {
        return new Ticker24HrEntry(ticker24Hr, time, time);
      }
      if (!takeStats && !takeFull) {
        return current;
      }
      Ticker24Hr merged = new Ticker24Hr();
      merged.setSymbol(symbol);
      copyStatsFields(takeStats ? ticker24Hr : current.ticker(), merged);
      copyFullTickerFields(takeFull ? ticker24Hr : current.ticker(), merged);
      return new Ticker24HrEntry(merged,
          takeStats ? time : current.statsTime(), takeFull ? time : current.fullTime());
    });
  }

  /**
   * The mini ticker carries OHLC, volumes and the last price. Bid/ask, last quantity, trade ids and the
   * previous close keep the values, and the time, of the last full ticker for the symbol.
   */
  private void mergeMiniTicker24Hr(EventMiniTicker24HrEvent event) {
    if (event.getSymbol() == null || event.getEventTime() == null) {
      return;
    }
    long time = event.getEventTime();
    ticker24HrCache.asMap().compute(event.getSymbol(), (symbol, current) -> {
      if (current != null && time < current.statsTime()) {
        return current;
      }
      Ticker24Hr merged = new Ticker24Hr();
      merged.setSymbol(symbol);
      if (current != null) {
        copyFullTickerFields(current.ticker(), merged);
      }
      applyMiniTickerStats(event, merged);
      return new Ticker24HrEntry(merged, time, current == null ? 0L : current.fullTime());
    });
  }

  private static void applyMiniTickerStats(EventMiniTicker24HrEvent event, Ticker24Hr ticker24Hr) {
    BigDecimal lastPrice = event.getLastPrice();
    BigDecimal openPrice = event.getOpenPrice();
    BigDecimal volume = event.getVolume();
    BigDecimal quoteVolume = event.getQuoteVolume();
    ticker24Hr.setLastPrice(lastPrice);
    ticker24Hr.setOpenPrice(openPrice);
    ticker24Hr.setHighPrice(event.getHighPrice());
    ticker24Hr.setLowPrice(event.getLowPrice());
    ticker24Hr.setVolume(volume);
    ticker24Hr.setQuoteVolume(quoteVolume);
    if (lastPrice != null && openPrice != null) {
      BigDecimal priceChange = lastPrice.subtract(openPrice);
      ticker24Hr.setPriceChange(priceChange);
      ticker24Hr.setPriceChangePercent(openPrice.signum() == 0 ? BigDecimal.ZERO
          : priceChange.multiply(ONE_HUNDRED).divide(openPrice, 3, RoundingMode.HALF_UP));
    }
    if (volume != null && quoteVolume != null) {
      ticker24Hr.setWeightedAvgPrice(volume.signum() == 0 ? BigDecimal.ZERO
          : quoteVolume.divide(volume, 8, RoundingMode.HALF_UP));
    }
    ticker24Hr.setOpenTime(event.getEventTime() - Duration.ofDays(1).toMillis());
    ticker24Hr.setCloseTime(event.getEventTime());
  }

  /** price and volume statistics: the fields the mini ticker also carries or implies */
  private static void copyStatsFields(Ticker24Hr from, Ticker24Hr to) {
    to.setPriceChange(from.getPriceChange());
    to.setPriceChangePercent(from.getPriceChangePercent());
    to.setWeightedAvgPrice(from.getWeightedAvgPrice());
    to.setLastPrice(from.getLastPrice());
    to.setOpenPrice(from.getOpenPrice());
    to.setHighPrice(from.getHighPrice());
    to.setLowPrice(from.getLowPrice());
    to.setVolume(from.getVolume());
    to.setQuoteVolume(from.getQuoteVolume());
    to.setOpenTime(from.getOpenTime());
    to.setCloseTime(from.getCloseTime());
  }

  /** fields only a full ticker carries */
  private static void copyFullTickerFields(Ticker24Hr from, Ticker24Hr to) {
    to.setPrevClosePrice(from.getPrevClosePrice());
    to.setLastQty(from.getLastQty());
    to.setBidPrice(from.getBidPrice());
    to.setBidQty(from.getBidQty());
    to.setAskPrice(from.getAskPrice());
    to.setAskQty(from.getAskQty());
    to.setFirstId(from.getFirstId());
    to.setLastId(from.getLastId());
    to.setCount(from.getCount());
  }

  protected Function<ParsedWebSocketMessage, String> getKlineEventMessageTopicExtractor() {
    return parsedMessage -> {
      AbstractBinanceKlineStream stream = getKlineStreamForEvent(parsedMessage.eventType());
      return stream != null ? stream.extractTopic(parsedMessage) : null;
    };
  }

  protected Function<ParsedWebSocketMessage, String> getTicker24HrEventMessageTopicExtractor() {
    return parsedMessage -> {
      AllMarketTickerStream stream = getAllMarketTickerStream();
      JsonNode payloadNode = parsedMessage.payloadNode();
      if (payloadNode == null || payloadNode.isNull()
          || !StringUtils.equals(parsedMessage.eventType(), stream.eventType())) {
        return null;
      }
      if (payloadNode.isArray()) {
        return allEventTypesMatch(payloadNode, stream.eventType()) ? stream.topic() : null;
      }
      return StringUtils.isNotBlank(parsedMessage.stream()) ? parsedMessage.stream() : stream.topic();
    };
  }

  protected String buildSymbolUpdateTopic(String symbol, String interval) {
    return ORDINARY_KLINE_STREAM.subscriptionTopic(symbol, interval);
  }

  protected String buildKlineSubscriptionTopic(String symbol, String interval) {
    String topic = getKlineStreamForInterval(interval).subscriptionTopic(symbol, interval);
    // Unsupported or ambiguous metadata must not substitute another contract's history.
    return topic != null ? topic : buildSymbolUpdateTopic(symbol, interval);
  }

  private Map<String, StreamSubscriptionState> getKlineSubscriptionState() {
    return getSubscribeIntervals().stream().collect(Collectors.toUnmodifiableMap(IntervalEnum::code, interval -> {
      AbstractBinanceKlineStream stream = getKlineStreamForInterval(interval.code());
      return new StreamSubscriptionState(stream.eventType(), stream.subscriptionState());
    }));
  }

  protected void start() {
    if (!Boolean.TRUE.equals(getSyncConfig().getEnabled())) {
      log.info("kline service {} disabled.", getClass().getSimpleName());
      return;
    }
    expectedTopicsSnapshot = null;
    Set<KlineSetKey> configuredKlineSetKeys = buildConfiguredKlineSetKeys();
    Set<KlineSetKey> restoredKlineSetKeys = restorePersistedKlines(configuredKlineSetKeys);
    Set<String> expectTopics = buildExpectedTopics();
    startKlineWebSocketUpdater(expectTopics.size());
    startPersistedKlineWarmup(restoredKlineSetKeys, configuredKlineSetKeys);
    startKlineRpcUpdater();
    startTicker24HrRpcUpdater();
    startTickerPriceCompensation();
    startAllMarketSnapshotUpdater();
    startSymbolOfflineCleaner();
    startKlinePersistenceUpdater();
  }

  protected void registerExtraTopic(String topic) {
    this.extraSubscribeTopics.add(topic);
    expectedTopicsSnapshot = null;
  }

  protected void unregisterExtraTopic(String topic) {
    this.extraSubscribeTopics.remove(topic);
    expectedTopicsSnapshot = null;
  }

  private List<ImmutablePair<Long, Long>> buildMakeUpTimeRanges(String symbol, Long startTime, Long endTime, IntervalEnum intervalEnum, Integer limit, boolean useSetCache) {
    ImmutablePair<Long, Long> realTimePair = calculateRealStartEndTime(startTime, endTime, intervalEnum, limit);
    long intervalMills = intervalEnum.getMills();
    Long realStartTime = realTimePair.getLeft();
    Long realEndTime = realTimePair.getRight();

    NavigableSet<Long> needMakeUpOpenTimes = new TreeSet<>();
    long indexTime = realStartTime;
    while (indexTime <= realEndTime) {
      needMakeUpOpenTimes.add(indexTime);
      indexTime += intervalMills;
    }
    KlineSetKey key = new KlineSetKey(symbol, intervalEnum.code());
    if (useSetCache) {
      KlineSet klineSet = klineSetMap.computeIfAbsent(key, var -> new KlineSet(key));
      Map<Long, Kline> savedKlineMap = klineSet.getKlineMap()
          .subMap(realStartTime, true, realEndTime, true);
      needMakeUpOpenTimes.removeAll(savedKlineMap.keySet());
    }

    List<ImmutablePair<Long, Long>> makeUpTimeRanges = new ArrayList<>();
    if (CollectionUtils.isNotEmpty(needMakeUpOpenTimes)) {
      if (needMakeUpOpenTimes.size() > 1) {
        Long makeStartTime = null;
        Long makeEndTime = null;
        for (Long needMakeUpOpenTime : needMakeUpOpenTimes) {
          if (makeStartTime == null) {
            makeStartTime = needMakeUpOpenTime;
            makeEndTime = makeStartTime + ((calculateRealLimit(getMakeUpKlinesLimit()) - 1) * intervalMills);
            makeUpTimeRanges.add(ImmutablePair.of(makeStartTime, makeEndTime));
            continue;
          }
          if (needMakeUpOpenTime > makeEndTime) {
            makeStartTime = needMakeUpOpenTime;
            makeEndTime = makeStartTime + ((calculateRealLimit(getMakeUpKlinesLimit()) - 1) * intervalMills);
            makeUpTimeRanges.add(ImmutablePair.of(makeStartTime, makeEndTime));
          }
        }
      } else {
        Long makeUpKlineTime = needMakeUpOpenTimes.first();
        makeUpTimeRanges.add(ImmutablePair.of(makeUpKlineTime, makeUpKlineTime + 1));
      }
    }
    return makeUpTimeRanges;
  }

  private int calculateRealLimit(Integer limit) {
    int realLimit = limit != null ? limit : DEFAULT_LIMIT;
    realLimit = Math.min(realLimit, MAX_LIMIT);
    return Math.max(realLimit, MIN_LIMIT);
  }

  private long calculateKlinesDuration(IntervalEnum intervalEnum, int realLimit) {
    return intervalEnum.getMills() * (realLimit - 1);
  }

  private ImmutablePair<Long, Long> calculateRealStartEndTime(Long startTime, Long endTime, IntervalEnum intervalEnum, int realLimit) {
    long intervalMills = intervalEnum.getMills();
    long klinesDuration = calculateKlinesDuration(intervalEnum, realLimit);
    long realStartTime;
    long realEndTime;
    if (startTime != null && endTime != null) {
      realStartTime = (long) Math.ceil((double) startTime / (double) intervalMills) * intervalMills;
      realEndTime = (long) Math.floor((double) endTime / (double) intervalMills) * intervalMills;
      long maxEndTime = realStartTime + klinesDuration;
      realEndTime = Math.min(realEndTime, maxEndTime);
    } else if (startTime == null && endTime == null) {
      realEndTime = (long) Math.floor((double) System.currentTimeMillis() / (double) intervalMills) * intervalMills;
      realStartTime = realEndTime - klinesDuration;
    } else if (startTime == null) {
      realEndTime = (long) Math.floor((double) endTime / (double) intervalMills) * intervalMills;
      realStartTime = realEndTime - klinesDuration;
    } else {
      // endTime == null
      realStartTime = (long) Math.ceil((double) startTime / (double) intervalMills) * intervalMills;
      realEndTime = realStartTime + klinesDuration;
    }
    return ImmutablePair.of(realStartTime, realEndTime);
  }

  private List<String> getSubscribeSymbols(IntervalEnum interval) {
    return getSubscribeSymbols(getSymbols(), interval);
  }

  private List<String> getSubscribeSymbols(Collection<String> symbols, IntervalEnum interval) {
    List<Pattern> subscribeSymbolPatterns = getSubscribeSymbolPatterns(interval);
    return symbols.stream()
        .filter(symbol -> {
          for (Pattern pattern : subscribeSymbolPatterns) {
            if (pattern.matcher(symbol).matches()) {
              return true;
            }
          }
          return false;
        }).toList();
  }

  private void startKlineWebSocketUpdater(int topicCount) {
    int connectionCountNumber = Math.min(webSocketClients.size(), (int) Math.ceil((double) topicCount / (double) SYMBOLS_PER_CONNECTION));
    connectionCountNumber = Math.max(connectionCountNumber, 1);
    connectionCount.set(connectionCountNumber);
    for(int i = 0; i < connectionCount.get(); i++) {
      T webSocketClient = webSocketClients.get(i);
      webSocketClient.addMessageHandler(getKlineEventMessageHandler());
      webSocketClient.setKlineMessageClassifier(getKlineMessageClassifier());
      webSocketClient.addMessageHandler(getTicker24HrEventMessageHandler());
      webSocketClient.addMessageTopicExtractorHandler(getKlineEventMessageTopicExtractor());
      webSocketClient.addMessageTopicExtractorHandler(getTicker24HrEventMessageTopicExtractor());
      webSocketClient.start();
    }
    adjustSubscribeTopics();
  }

  protected void subscribe(Collection<String> topics) {
    processTopics(topics, WebSocketClient::subscribeTopics);
  }

  protected void unsubscribe(Collection<String> topics) {
    processTopics(topics, WebSocketClient::unsubscribeTopics);
  }

  protected void processTopics(Collection<String> topics, BiConsumer<WebSocketClient, List<String>> topicsConsumer) {
    List<T> clients = getActiveWebSocketClients();
    if (CollectionUtils.isEmpty(clients) || CollectionUtils.isEmpty(topics)) {
      return;
    }

    int clientsSize = clients.size();
    Map<Integer, List<String>> clientIndexTopicsMap = topics.stream()
        .collect(Collectors.groupingBy(topic -> Math.abs(topic.hashCode()) % clientsSize));
    clientIndexTopicsMap.forEach((clientIndex, subTopics) -> topicsConsumer.accept(clients.get(clientIndex), subTopics));
  }

  private Float toFloat(Object floatObj) {
    if (floatObj == null) {
      return null;
    }
    if (floatObj instanceof Float floatValue) {
      return floatValue;
    } else if (floatObj instanceof String doubleStr) {
      return Float.valueOf(doubleStr);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private Double toDouble(Object doubleObj) {
    if (doubleObj == null) {
      return null;
    }
    if (doubleObj instanceof Double doubleValue) {
      return doubleValue;
    } else if (doubleObj instanceof String doubleStr) {
      return Double.valueOf(doubleStr);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private BigDecimal toBigDecimal(Object bigDecimalObj) {
    if (bigDecimalObj == null) {
      return null;
    }
    if (bigDecimalObj instanceof BigDecimal bigDecimalValue) {
      return bigDecimalValue;
    } else if (bigDecimalObj instanceof String bigDecimalStr) {
      return new BigDecimal(bigDecimalStr);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  protected Kline serverKlineToKline(Object[] serverKline) {
    if (serverKline == null) {
      return null;
    }

    NumberTypeEnum numberTypeEnum = getNumberType();
    switch (numberTypeEnum) {
      case STRING -> {
        StringKline kline = new StringKline();
        kline.setOpenTime((Long) serverKline[0]);
        kline.setOpenPrice((String) serverKline[1]);
        kline.setHighPrice((String) serverKline[2]);
        kline.setLowPrice((String) serverKline[3]);
        kline.setClosePrice((String) serverKline[4]);
        kline.setVolume((String) serverKline[5]);
        kline.setCloseTime((Long) serverKline[6]);
        kline.setQuoteVolume((String) serverKline[7]);
        kline.setTradeNum((Integer) serverKline[8]);
        kline.setActiveBuyVolume((String) serverKline[9]);
        kline.setActiveBuyQuoteVolume((String) serverKline[10]);
        /*
        kline.setIgnore((String) serverKline[11]);
        */
        return kline;
      }
      case FLOAT -> {
        FloatKline kline = new FloatKline();
        kline.setOpenTime((Long) serverKline[0]);
        kline.setOpenPrice(toFloat(serverKline[1]));
        kline.setHighPrice(toFloat(serverKline[2]));
        kline.setLowPrice(toFloat(serverKline[3]));
        kline.setClosePrice(toFloat(serverKline[4]));
        kline.setVolume(toFloat(serverKline[5]));
        kline.setCloseTime((Long) serverKline[6]);
        kline.setQuoteVolume(toFloat(serverKline[7]));
        kline.setTradeNum((Integer) serverKline[8]);
        kline.setActiveBuyVolume(toFloat(serverKline[9]));
        kline.setActiveBuyQuoteVolume(toFloat(serverKline[10]));
        /*
        kline.setIgnore((String) serverKline[11]);
        */
        return kline;
      }
      case DOUBLE -> {
        DoubleKline kline = new DoubleKline();
        kline.setOpenTime((Long) serverKline[0]);
        kline.setOpenPrice(toDouble(serverKline[1]));
        kline.setHighPrice(toDouble(serverKline[2]));
        kline.setLowPrice(toDouble(serverKline[3]));
        kline.setClosePrice(toDouble(serverKline[4]));
        kline.setVolume(toDouble(serverKline[5]));
        kline.setCloseTime((Long) serverKline[6]);
        kline.setQuoteVolume(toDouble(serverKline[7]));
        kline.setTradeNum((Integer) serverKline[8]);
        kline.setActiveBuyVolume(toDouble(serverKline[9]));
        kline.setActiveBuyQuoteVolume(toDouble(serverKline[10]));
        /*
        kline.setIgnore((String) serverKline[11]);
        */
        return kline;
      }
      case BIG_DECIMAL -> {
        BigDecimalKline kline = new BigDecimalKline();
        kline.setOpenTime((Long) serverKline[0]);
        kline.setOpenPrice(toBigDecimal(serverKline[1]));
        kline.setHighPrice(toBigDecimal(serverKline[2]));
        kline.setLowPrice(toBigDecimal(serverKline[3]));
        kline.setClosePrice(toBigDecimal(serverKline[4]));
        kline.setVolume(toBigDecimal(serverKline[5]));
        kline.setCloseTime((Long) serverKline[6]);
        kline.setQuoteVolume(toBigDecimal(serverKline[7]));
        kline.setTradeNum((Integer) serverKline[8]);
        kline.setActiveBuyVolume(toBigDecimal(serverKline[9]));
        kline.setActiveBuyQuoteVolume(toBigDecimal(serverKline[10]));
        /*
        kline.setIgnore((String) serverKline[11]);
        */
        return kline;
      }
      default -> {
        throw new UnsupportedOperationException();
      }
    }
  }

  protected int getLimit(Integer originalLimit) {
    if (originalLimit == null) {
      return DEFAULT_LIMIT;
    }
    int limit = originalLimit;
    limit = Math.max(MIN_LIMIT, limit);
    limit = Math.min(MAX_LIMIT, limit);
    return limit;
  }

  private Long querySymbolOnboardTime(String symbol) {
    return symbolOnboardTimeMap.computeIfAbsent(symbol, var -> {
      Kline kline = querySymbolFirstKline(symbol, IntervalEnum.ONE_MINUTE.code());
      if (kline == null) {
        return -1L;
      }
      return kline.getOpenTime();
    });
  }

  private Kline querySymbolFirstKline(String symbol, String interval) {
    rateLimitManager.acquire(getRateLimiterName(), getMakeUpKlinesWeight());
    List<Kline> subKlines = queryKlines0(symbol, interval,
        0L, null, 1);
    if (CollectionUtils.isEmpty(subKlines)) {
      return null;
    }
    return subKlines.get(0);
  }

  public List<Ticker24Hr> queryTicker24Hrs() {
    rateLimitManager.acquire(getRateLimiterName(), getTicker24HrsWeight());
    List<Ticker24Hr> ticker24Hrs = queryTicker24Hrs0();
    return ticker24Hrs == null ? List.of() : ticker24Hrs;
  }

  private void startAllMarketSnapshotUpdater() {
    SCHEDULE_EXECUTOR_SERVICE.scheduleWithFixedDelay(new ExceptionSafeRunnable(this::logIncompleteSettles),
        5_000L, 5_000L, TimeUnit.MILLISECONDS);
    SCHEDULE_EXECUTOR_SERVICE.scheduleWithFixedDelay(
        new ExceptionSafeRunnable(() -> {
          long now = System.currentTimeMillis();
          if (!isTickerStreamAlive()) {
            refreshAllMarketSnapshotIfActive(now, lastAllMarketTickerAccessTime, provisionalTickerSnapshot,
                this::triggerProvisionalTickerRefresh);
          }
          refreshAllMarketSnapshotIfActive(now, lastAllMarketTicker24HrAccessTime, allMarketTicker24HrSnapshot,
              this::triggerAllMarketTicker24HrRefresh);
        }), 1000, ALL_MARKET_SNAPSHOT_REFRESH_CHECK_INTERVAL_MILLS, TimeUnit.MILLISECONDS);
  }

  /** REST compensation for changes the stream never delivered; an older price never replaces a newer one. */
  private void startTickerPriceCompensation() {
    SCHEDULE_EXECUTOR_SERVICE.scheduleWithFixedDelay(new ExceptionSafeRunnable(() -> {
      if (syncTickerPrices()) {
        scheduleTickerPriceResyncIfNeeded();
      }
    }), 1000, tickerRestCompensationIntervalMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Apply one full-market snapshot, every price dated by its last update, to the price book.
   * @return true when a non-empty snapshot was applied
   */
  private boolean syncTickerPrices() {
    synchronized (tickerPriceSyncLock) {
      lastTickerPriceSyncAttemptTime.set(System.currentTimeMillis());
      long requestTime = getServerTime();
      if (isTickerPriceSnapshotFrom24Hr()) {
        List<Ticker24Hr> ticker24Hrs = queryTicker24Hrs();
        if (CollectionUtils.isEmpty(ticker24Hrs)) {
          return false;
        }
        publishTicker24HrSnapshot(ticker24Hrs, requestTime);
        return true;
      }
      List<Ticker<?>> tickers = queryTickers0();
      if (CollectionUtils.isEmpty(tickers)) {
        return false;
      }
      tickerPriceBook.applySnapshot(tickers, requestTime);
      return true;
    }
  }

  /**
   * A read without a snapshot joins the one in flight, or takes one unless one was just attempted.
   * A failure is left to the caller's no-snapshot policy (-1001, or dated symbol lookups).
   */
  private void syncTickerPricesIfNoSnapshot() {
    synchronized (tickerPriceSyncLock) {
      if (tickerPriceBook.hasFullSnapshot() || isTickerPriceSyncAttemptRecent()) {
        return;
      }
      try {
        syncTickerPrices();
      } catch (RuntimeException e) {
        log.warn("service: {} first ticker price snapshot failed.", getServiceType(), e);
      }
    }
  }

  private boolean isTickerPriceSyncAttemptRecent() {
    return System.currentTimeMillis() - lastTickerPriceSyncAttemptTime.get() < ALL_MARKET_TICKER_CACHE_TTL.toMillis();
  }

  private void triggerTickerPriceSyncIfStale() {
    if (!isTickerPriceSyncAttemptRecent()) {
      triggerTickerPriceSync();
    }
  }

  /** Repair an uncovered stream segment; skipped when another snapshot covered it meanwhile. */
  private void triggerTickerPriceSync() {
    triggerSingleFlight(syncingTickerPrices, () -> {
      boolean applied;
      synchronized (tickerPriceSyncLock) {
        applied = tickerPriceBook.needsFullSync() && syncTickerPrices();
      }
      if (applied) {
        scheduleTickerPriceResyncIfNeeded();
      }
    });
  }

  /** A snapshot requested too soon after a stream segment started does not cover it; take another one. */
  private void scheduleTickerPriceResyncIfNeeded() {
    if (tickerPriceBook.needsFullSync()) {
      SCHEDULE_EXECUTOR_SERVICE.schedule(new ExceptionSafeRunnable(this::triggerTickerPriceSync),
          ALL_MARKET_TICKER_CACHE_TTL.toMillis(), TimeUnit.MILLISECONDS);
    }
  }

  private void triggerSingleFlight(AtomicBoolean running, Runnable task) {
    if (!running.compareAndSet(false, true)) {
      return;
    }
    try {
      MANAGE_EXECUTOR.execute(() -> {
        try {
          task.run();
        } catch (RuntimeException e) {
          log.warn("service: {} background refresh failed.", getServiceType(), e);
        } finally {
          running.set(false);
        }
      });
    } catch (RejectedExecutionException e) {
      running.set(false);
      log.warn("service: {} background refresh rejected.", getServiceType(), e);
    }
  }

  /**
   * REST ticker/price as fetched, served while the stream is silent. A snapshot nobody kept fresh
   * (no all-market read for longer than the idle timeout) is refreshed before it is served.
   */
  private List<Ticker<?>> queryProvisionalAllMarketTickers() {
    long now = System.currentTimeMillis();
    AllMarketSnapshot<Ticker<?>> snapshot = provisionalTickerSnapshot.get();
    if (CollectionUtils.isNotEmpty(snapshot.values())
        && now - snapshot.refreshedAt() <= ALL_MARKET_SNAPSHOT_REFRESH_IDLE_TIMEOUT_MILLS
        && !isProvisionalBehindStream(snapshot)) {
      if (snapshot.isStale(now)) {
        triggerProvisionalTickerRefresh();
      }
      return snapshot.values();
    }
    refreshProvisionalTickers();
    return provisionalTickerSnapshot.get().values();
  }

  /** stream frames after the request carry prices the passthrough lacks: the book may be newer */
  private boolean isProvisionalBehindStream(AllMarketSnapshot<Ticker<?>> snapshot) {
    return tickerPriceBook.lastStreamEventTime() > snapshot.streamMark();
  }

  private void triggerProvisionalTickerRefresh() {
    triggerSingleFlight(refreshingProvisionalTickerSnapshot, this::refreshProvisionalTickers);
  }

  /** Readers and the background refresh share one refresh: whoever waited reuses its result. */
  private void refreshProvisionalTickers() {
    synchronized (provisionalTickerLock) {
      AllMarketSnapshot<Ticker<?>> current = provisionalTickerSnapshot.get();
      if (current.refreshedAt() != 0L && !current.isStale(System.currentTimeMillis())
          && !isProvisionalBehindStream(current)) {
        return;
      }
      long streamMark = tickerPriceBook.lastStreamEventTime();
      List<Ticker<?>> tickers = queryTickers0();
      List<Ticker<?>> values = new ArrayList<>(tickers == null ? 0 : tickers.size());
      if (tickers != null) {
        tickerPriceBook.applySymbols(tickers);  // dated prices (futures) still feed the book
        for (Ticker<?> ticker : tickers) {
          if (ticker.getSymbol() != null && ticker.getPrice() != null) {
            values.add(newerOfRestAndBook(ticker));
          }
        }
      }
      provisionalTickerSnapshot.set(
          new AllMarketSnapshot<>(List.copyOf(values), System.currentTimeMillis(), streamMark));
    }
  }

  private <E> void refreshAllMarketSnapshotIfActive(long now, AtomicLong lastAccessTime,
                                                    AtomicReference<AllMarketSnapshot<E>> snapshotRef,
                                                    Runnable refreshTask) {
    if (now - lastAccessTime.get() > ALL_MARKET_SNAPSHOT_REFRESH_IDLE_TIMEOUT_MILLS) {
      return;
    }
    AllMarketSnapshot<E> snapshot = snapshotRef.get();
    if (!snapshot.isStale(now)) {
      return;
    }
    refreshTask.run();
  }

  private void triggerAllMarketTicker24HrRefreshIfStale(AllMarketSnapshot<Ticker24Hr> snapshot) {
    if (snapshot.isStale(System.currentTimeMillis())) {
      triggerAllMarketTicker24HrRefresh();
    }
  }

  private void triggerAllMarketTicker24HrRefresh() {
    triggerSingleFlight(refreshingAllMarketTicker24HrSnapshot, this::refreshAllMarketTicker24HrSnapshot);
  }

  private List<Ticker24Hr> refreshAllMarketTicker24HrsSnapshotNow() {
    refreshAllMarketTicker24HrSnapshot();
    return allMarketTicker24HrSnapshot.get().values();
  }

  private void refreshAllMarketTicker24HrSnapshot() {
    long requestTime = getServerTime();
    List<Ticker24Hr> ticker24Hrs = queryTicker24Hrs();
    if (CollectionUtils.isEmpty(ticker24Hrs)) {
      allMarketTicker24HrSnapshot.set(new AllMarketSnapshot<>(List.of(), System.currentTimeMillis()));
      return;
    }
    publishTicker24HrSnapshot(ticker24Hrs, requestTime);
  }

  /** @param requestTime server time just before the ticker/24hr request was sent */
  private void publishTicker24HrSnapshot(List<Ticker24Hr> ticker24Hrs, long requestTime) {
    allMarketTicker24HrSnapshot.set(new AllMarketSnapshot<>(List.copyOf(ticker24Hrs), System.currentTimeMillis()));
    syncTicker24HrCache(ticker24Hrs);
    if (isTickerPriceSnapshotFrom24Hr()) {
      tickerPriceBook.applySnapshot(toPriceTickers(ticker24Hrs), requestTime);
    }
  }

  /** ticker/24hr closeTime is the symbol's last update time, the date lastPrice needs */
  private static List<Ticker<?>> toPriceTickers(List<Ticker24Hr> ticker24Hrs) {
    List<Ticker<?>> tickers = new ArrayList<>(ticker24Hrs.size());
    for (Ticker24Hr ticker24Hr : ticker24Hrs) {
      if (ticker24Hr.getSymbol() == null || ticker24Hr.getLastPrice() == null || ticker24Hr.getCloseTime() == null) {
        continue;
      }
      BigDecimalTicker ticker = new BigDecimalTicker();
      ticker.setSymbol(ticker24Hr.getSymbol());
      ticker.setPrice(ticker24Hr.getLastPrice());
      ticker.setTime(ticker24Hr.getCloseTime());
      tickers.add(ticker);
    }
    return tickers;
  }

  private void syncTicker24HrCache(List<Ticker24Hr> ticker24Hrs) {
    if (CollectionUtils.isEmpty(ticker24Hrs)) {
      return;
    }
    Set<String> symbols = new HashSet<>();
    for (Ticker24Hr ticker24Hr : ticker24Hrs) {
      mergeTicker24Hr(ticker24Hr);
      symbols.add(ticker24Hr.getSymbol());
    }
    ticker24HrCache.asMap().keySet().retainAll(symbols);
  }

  private List<Ticker24Hr> queryAllMarketTicker24HrsSnapshot() {
    lastAllMarketTicker24HrAccessTime.set(System.currentTimeMillis());
    AllMarketSnapshot<Ticker24Hr> snapshot = allMarketTicker24HrSnapshot.get();
    if (CollectionUtils.isNotEmpty(snapshot.values())) {
      triggerAllMarketTicker24HrRefreshIfStale(snapshot);
      return snapshot.values();
    }
    return refreshAllMarketTicker24HrsSnapshotNow();
  }

  private List<Kline> safeQueryKlines(String symbol, String interval, Long startTime, Long endTime, Integer limit) {
    return safeQueryKlines(symbol, interval, startTime, endTime, limit, MAX_MAKE_UP_WORKERS, KLINE_FETCH_EXECUTOR);
  }

  private List<Kline> safeQueryKlines(String symbol, String interval, Long startTime, Long endTime,
                                      Integer limit, int maxWorkers, Executor executor) {
    IntervalEnum intervalEnum = CommonUtil.getEnumByCode(interval, IntervalEnum.class);
    List<ImmutablePair<Long, Long>> makeUpTimeRanges = buildMakeUpTimeRanges(symbol, startTime,
        endTime, intervalEnum, limit, false);
    makeUpTimeRanges = filterMakeUpTimeRangesByOnboardTime(symbol, makeUpTimeRanges);

    return fetchAndStoreKlines(symbol, interval, makeUpTimeRanges, maxWorkers, executor);
  }

  private List<ImmutablePair<Long, Long>> filterMakeUpTimeRangesByOnboardTime(String symbol,
                                                                               List<ImmutablePair<Long, Long>> makeUpTimeRanges) {
    if (CollectionUtils.isEmpty(makeUpTimeRanges)) {
      return Collections.emptyList();
    }
    Long symbolOnboardTime = querySymbolOnboardTime(symbol);
    if (symbolOnboardTime < 0) {
      return makeUpTimeRanges;
    }
    return makeUpTimeRanges.stream()
        .filter(range -> range.getRight() >= symbolOnboardTime)
        .collect(Collectors.toList());
  }

  private List<Kline> fetchAndStoreKlines(String symbol, String interval,
                                          List<ImmutablePair<Long, Long>> makeUpTimeRanges,
                                          int maxWorkers, Executor executor) {
    if (CollectionUtils.isEmpty(makeUpTimeRanges)) {
      return Collections.emptyList();
    }

    List<Kline> fetchedKlines = Collections.synchronizedList(new ArrayList<>());
    List<Runnable> fetchTasks = makeUpTimeRanges.stream()
        .<Runnable>map(rangePair -> () -> {
        rateLimitManager.acquire(getRateLimiterName(), getMakeUpKlinesWeight());
        List<Kline> subKlines = queryKlines0(symbol, interval,
            rangePair.getLeft(), rangePair.getRight(), getMakeUpKlinesLimit());
        for (Kline makeUpKline : subKlines) {
          updateKline(symbol, interval, makeUpKline);
        }
        fetchedKlines.addAll(subKlines);
      })
        .toList();
    runTasksWithLimitedWorkers(fetchTasks, maxWorkers, executor);
    return fetchedKlines.stream()
        .sorted(Comparator.comparing(Kline::getOpenTime))
        .toList();
  }

  private void startSymbolOfflineCleaner() {
    SCHEDULE_EXECUTOR_SERVICE.scheduleWithFixedDelay(
        new ExceptionSafeRunnable(() -> {
          Set<String> tradingSymbols = new HashSet<>(getSymbols());
          List<KlineSetKey> offlineKlineSetKeys = klineSetMap.keySet().stream()
              .filter(klineSetKey -> !tradingSymbols.contains(klineSetKey.getSymbol()))
              .toList();
          if (CollectionUtils.isEmpty(offlineKlineSetKeys)) {
            return;
          }
          for (KlineSetKey klineSetKey : offlineKlineSetKeys) {
            klineSetMap.remove(klineSetKey);
            cleanupPersistedKlines(klineSetKey);
            log.info("symbol: {} offline, kline set of interval: {} removed", klineSetKey.getSymbol(), klineSetKey.getInterval());
          }
        }), 1000, 1000 * 60 * 5, TimeUnit.MILLISECONDS);
  }

  private void startTicker24HrRpcUpdater() {
    if (isTickerPriceSnapshotFrom24Hr()) {
      return;  // the ticker price compensation fetches ticker/24hr for this market
    }
    SCHEDULE_EXECUTOR_SERVICE.scheduleWithFixedDelay(
        new ExceptionSafeRunnable(() -> {
          long requestTime = getServerTime();
          List<Ticker24Hr> ticker24Hrs = queryTicker24Hrs();
          if (CollectionUtils.isEmpty(ticker24Hrs)) {
            return;
          }
          publishTicker24HrSnapshot(ticker24Hrs, requestTime);
        }), 1000, 1000 * 60, TimeUnit.MILLISECONDS
    );
  }

  private void startKlineRpcUpdater() {
    SCHEDULE_EXECUTOR_SERVICE.scheduleWithFixedDelay(
        new ExceptionSafeRunnable(this::syncConfiguredKlinesOnce), 1000, 1000 * 60 * 5, TimeUnit.MILLISECONDS);
  }

  /**
   * Keep the ~90 s full RPC sync away from HH:00 (see {@link HourBoundaryGuard}): a sync
   * straddling the boundary delayed the fleet's closed-bar reads by 0.2–1.1 s in ~25% of
   * hours. Skipping is safe — the sync only back-fills gaps; the WebSocket stream keeps the
   * live candle current — and the next fixed-delay tick (5 min) is outside the window.
   */
  @Value("${kline.rpcSync.hourBoundaryGuardBeforeMs:150000}")
  private long rpcSyncHourBoundaryGuardBeforeMs = 150_000L;

  @Value("${kline.rpcSync.hourBoundaryGuardAfterMs:30000}")
  private long rpcSyncHourBoundaryGuardAfterMs = 30_000L;

  private void syncConfiguredKlinesOnce() {
    long now = getServerTime();
    if (HourBoundaryGuard.shouldSkip(now, rpcSyncHourBoundaryGuardBeforeMs, rpcSyncHourBoundaryGuardAfterMs)) {
      log.info("klines rpc sync for {} skipped: {} ms into the hour is inside the hour-boundary guard (before={} ms, after={} ms)",
          getClass().getSimpleName(), Math.floorMod(now, HourBoundaryGuard.HOUR_MS),
          rpcSyncHourBoundaryGuardBeforeMs, rpcSyncHourBoundaryGuardAfterMs);
      return;
    }
    List<IntervalEnum> subscribeIntervals = getSubscribeIntervals();
    List<String> symbols = getSymbols();
    List<ImmutablePair<String, IntervalEnum>> symbolIntervals = new ArrayList<>();
    for (IntervalEnum interval : subscribeIntervals) {
      List<String> subscribeSymbols = getSubscribeSymbols(symbols, interval);
      for (String symbol : subscribeSymbols) {
        symbolIntervals.add(ImmutablePair.of(symbol, interval));
      }
    }
    CompletableFuture.runAsync(() -> {
      List<Runnable> syncTasks = symbolIntervals.stream()
          .<Runnable>map(symbolInterval -> () -> {
            String symbol = symbolInterval.getLeft();
            IntervalEnum interval = symbolInterval.getRight();
            int refreshKlineCount = getBackgroundRefreshKlineCount(symbol, interval);
            safeQueryKlines(symbol, interval.code(),
                null, System.currentTimeMillis(),
                refreshKlineCount, 1, KLINE_FETCH_EXECUTOR);
          })
          .toList();
      runTasksWithLimitedWorkers(syncTasks, MAX_RPC_SYNC_WORKERS, KLINE_FETCH_EXECUTOR);
      log.info("klines for {} with intervals: {} synced.", getClass().getSimpleName(), subscribeIntervals);
    }, MANAGE_EXECUTOR).join();
  }

  private void startKlinePersistenceUpdater() {
    if (!isPersistenceEnabled()) {
      return;
    }
    long dumpIntervalSeconds = Math.max(1, persistenceProperties.getDumpIntervalSeconds());
    SCHEDULE_EXECUTOR_SERVICE.scheduleWithFixedDelay(
        new ExceptionSafeRunnable(() -> dumpPersistedKlines(false)),
        dumpIntervalSeconds, dumpIntervalSeconds, TimeUnit.SECONDS
    );
  }

  private Set<String> buildNeedSubscribeKlineUpdateTopics(Collection<String> needSubscribeIntervals) {
    return buildNeedSubscribeKlineUpdateTopics(needSubscribeIntervals, getSymbols());
  }

  private Set<String> buildNeedSubscribeKlineUpdateTopics(Collection<String> needSubscribeIntervals,
                                                          Collection<String> symbols) {
    if (CollectionUtils.isEmpty(needSubscribeIntervals)) {
      return Collections.emptySet();
    }
    Set<String> topics = new HashSet<>(needSubscribeIntervals.size());
    for (String interval : needSubscribeIntervals) {
      List<String> subscribeSymbols = getSubscribeSymbols(symbols,
          CommonUtil.getEnumByCode(interval, IntervalEnum.class));
      for (String symbol : subscribeSymbols) {
        String topic = buildKlineSubscriptionTopic(symbol, interval);
        topics.add(topic);
      }
    }
    return topics;
  }

  protected Set<String> buildExpectedTopics() {
    Set<String> subscribeIntervals = getSubscribeIntervals().stream()
        .map(IntervalEnum::code)
        .collect(Collectors.toSet());
    Set<String> symbols = new HashSet<>(getSymbols());
    Set<String> extraTopics = Set.copyOf(extraSubscribeTopics);
    Set<String> tickerTopics = Set.copyOf(getTicker24HrSubscribeTopics());
    Map<String, StreamSubscriptionState> subscriptionState = getKlineSubscriptionState();
    ExpectedTopicsSnapshot snapshot = expectedTopicsSnapshot;
    if (snapshot != null && snapshot.matches(symbols, subscribeIntervals, extraTopics, tickerTopics,
        subscriptionState)) {
      return snapshot.expectedTopics();
    }

    Set<String> expectTopics = new HashSet<>(extraTopics);
    expectTopics.addAll(tickerTopics);
    expectTopics.addAll(buildNeedSubscribeKlineUpdateTopics(subscribeIntervals, symbols));
    Set<String> immutableExpectedTopics = Set.copyOf(expectTopics);
    expectedTopicsSnapshot = new ExpectedTopicsSnapshot(Set.copyOf(symbols),
        Set.copyOf(subscribeIntervals), extraTopics, tickerTopics, subscriptionState, immutableExpectedTopics);
    return immutableExpectedTopics;
  }

  private void adjustSubscribeTopics() {
    SCHEDULE_EXECUTOR_SERVICE.scheduleWithFixedDelay(
        new ExceptionSafeRunnable(() -> {
          synchronized (this) {
            Set<String> expectTopics = buildExpectedTopics();
            Set<String> subscribedTopics = getSubscribedTopics();

            Set<String> needNewSubscribeTopics = new HashSet<>(expectTopics);
            needNewSubscribeTopics.removeAll(subscribedTopics);

            Set<String> needUnsubscribeTopics = new HashSet<>(subscribedTopics);
            needUnsubscribeTopics.removeAll(expectTopics);

            if (CollectionUtils.isNotEmpty(needNewSubscribeTopics)) {
              subscribe(needNewSubscribeTopics);
            }

            if (CollectionUtils.isNotEmpty(needUnsubscribeTopics)) {
              unsubscribe(needUnsubscribeTopics);
            }
          }
        }), 1000, SUBSCRIBE_TOPICS_ADJUST_INTERVAL_MILLS, TimeUnit.MILLISECONDS);
  }

  private Set<String> getSubscribedTopics() {
    return webSocketClients.stream()
        .map(WebSocketClient::getSubscribedTopics)
        .flatMap(Collection::stream)
        .collect(Collectors.toSet());
  }

  private List<T> getActiveWebSocketClients() {
    return webSocketClients.subList(0, connectionCount.get());
  }

  private void trimKlinesIfNeeded(KlineSet klineSet, IntervalEnum intervalEnum) {
    IntervalSyncConfig intervalSyncConfig = getSyncConfig().getIntervalSyncConfigs().get(intervalEnum.code());
    if (intervalSyncConfig == null || intervalSyncConfig.getMinMaintainCount() == null) {
      return;
    }
    int minMaintainCount = getEffectiveMaintainCount(klineSet.getKey().getSymbol(), intervalEnum);
    NavigableMap<Long, Kline> klineMap = klineSet.getKlineMap();
    if (klineMap.size() <= minMaintainCount + KLINE_TRIM_BUFFER) {
      return;
    }
    synchronized (klineSet) {
      while (klineMap.size() > minMaintainCount) {
        klineMap.pollFirstEntry();
      }
      if (!klineMap.isEmpty()) {
        klineSet.dropFinalBefore(klineMap.firstKey());
      }
    }
  }

  private Kline convertToKline(EventKlineEvent<?, ?> event) {
    if (event == null || event.getEventKline() == null) {
      return null;
    }
    EventKline<?> eventKline = event.getEventKline();
    if (eventKline instanceof StringEventKline stringEventKline) {
      StringKline stringKline = new StringKline();
      stringKline.setOpenTime(stringEventKline.getOpenTime());
      stringKline.setCloseTime(stringEventKline.getCloseTime());
      stringKline.setOpenPrice(stringEventKline.getOpenPrice());
      stringKline.setHighPrice(stringEventKline.getHighPrice());
      stringKline.setLowPrice(stringEventKline.getLowPrice());
      stringKline.setClosePrice(stringEventKline.getClosePrice());
      stringKline.setVolume(stringEventKline.getVolume());
      stringKline.setQuoteVolume(stringEventKline.getQuoteVolume());
      stringKline.setTradeNum(stringEventKline.getTradeNum());
      stringKline.setActiveBuyVolume(stringEventKline.getActiveBuyVolume());
      stringKline.setActiveBuyQuoteVolume(stringEventKline.getActiveBuyQuoteVolume());
      /*
      stringKline.setIgnore(stringEventKline.getIgnore());
      */
      return stringKline;
    } else if (eventKline instanceof FloatEventKline floatEventKline) {
      FloatKline floatKline = new FloatKline();
      floatKline.setOpenTime(floatEventKline.getOpenTime());
      floatKline.setCloseTime(floatEventKline.getCloseTime());
      floatKline.setOpenPrice(toFloat(floatEventKline.getOpenPrice()));
      floatKline.setHighPrice(toFloat(floatEventKline.getHighPrice()));
      floatKline.setLowPrice(toFloat(floatEventKline.getLowPrice()));
      floatKline.setClosePrice(toFloat(floatEventKline.getClosePrice()));
      floatKline.setVolume(toFloat(floatEventKline.getVolume()));
      floatKline.setQuoteVolume(toFloat(floatEventKline.getQuoteVolume()));
      floatKline.setTradeNum(floatEventKline.getTradeNum());
      floatKline.setActiveBuyVolume(toFloat(floatEventKline.getActiveBuyVolume()));
      floatKline.setActiveBuyQuoteVolume(toFloat(floatEventKline.getActiveBuyQuoteVolume()));
      /*
      floatKline.setIgnore(floatEventKline.getIgnore());
      */
      return floatKline;
    } else if (eventKline instanceof DoubleEventKline doubleEventKline) {
      DoubleKline doubleKline = new DoubleKline();
      doubleKline.setOpenTime(doubleEventKline.getOpenTime());
      doubleKline.setCloseTime(doubleEventKline.getCloseTime());
      doubleKline.setOpenPrice(toDouble(doubleEventKline.getOpenPrice()));
      doubleKline.setHighPrice(toDouble(doubleEventKline.getHighPrice()));
      doubleKline.setLowPrice(toDouble(doubleEventKline.getLowPrice()));
      doubleKline.setClosePrice(toDouble(doubleEventKline.getClosePrice()));
      doubleKline.setVolume(toDouble(doubleEventKline.getVolume()));
      doubleKline.setQuoteVolume(toDouble(doubleEventKline.getQuoteVolume()));
      doubleKline.setTradeNum(doubleEventKline.getTradeNum());
      doubleKline.setActiveBuyVolume(toDouble(doubleEventKline.getActiveBuyVolume()));
      doubleKline.setActiveBuyQuoteVolume(toDouble(doubleEventKline.getActiveBuyQuoteVolume()));
      /*
      doubleKline.setIgnore(doubleEventKline.getIgnore());
      */
      return doubleKline;
    } else if(eventKline instanceof BigDecimalEventKline bigDecimalEventKline){
      BigDecimalKline bigDecimalKline = new BigDecimalKline();
      bigDecimalKline.setOpenTime(bigDecimalEventKline.getOpenTime());
      bigDecimalKline.setCloseTime(bigDecimalEventKline.getCloseTime());
      bigDecimalKline.setOpenPrice(toBigDecimal(bigDecimalEventKline.getOpenPrice()));
      bigDecimalKline.setHighPrice(toBigDecimal(bigDecimalEventKline.getHighPrice()));
      bigDecimalKline.setLowPrice(toBigDecimal(bigDecimalEventKline.getLowPrice()));
      bigDecimalKline.setClosePrice(toBigDecimal(bigDecimalEventKline.getClosePrice()));
      bigDecimalKline.setVolume(toBigDecimal(bigDecimalEventKline.getVolume()));
      bigDecimalKline.setQuoteVolume(toBigDecimal(bigDecimalEventKline.getQuoteVolume()));
      bigDecimalKline.setTradeNum(bigDecimalEventKline.getTradeNum());
      bigDecimalKline.setActiveBuyVolume(toBigDecimal(bigDecimalEventKline.getActiveBuyVolume()));
      bigDecimalKline.setActiveBuyQuoteVolume(toBigDecimal(bigDecimalEventKline.getActiveBuyQuoteVolume()));
      /*
      bigDecimalKline.setIgnore(bigDecimalEventKline.getIgnore());
      */
      return bigDecimalKline;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private <E> List<E> convertToTickerEvents(ParsedWebSocketMessage parsedMessage, Class<E> eventClass) {
    JsonNode payloadNode = parsedMessage.payloadNode();
    if (payloadNode == null || payloadNode.isNull()) {
      return null;
    }
    if (!payloadNode.isArray()) {
      E event = serializer.treeToValue(payloadNode, eventClass);
      return event == null ? null : List.of(event);
    }
    List<E> events = new ArrayList<>(payloadNode.size());
    for (JsonNode eventNode : payloadNode) {
      E event = serializer.treeToValue(eventNode, eventClass);
      if (event != null) {
        events.add(event);
      }
    }
    return events;
  }

  protected static void runTasksWithLimitedWorkers(List<Runnable> tasks, int maxWorkers, Executor executor) {
    if (CollectionUtils.isEmpty(tasks)) {
      return;
    }
    int workerCount = Math.max(1, Math.min(maxWorkers, tasks.size()));
    if (workerCount == 1) {
      tasks.forEach(Runnable::run);
      return;
    }
    ConcurrentLinkedQueue<Runnable> taskQueue = new ConcurrentLinkedQueue<>(tasks);
    CompletableFuture<?>[] workers = new CompletableFuture[workerCount];
    for (int i = 0; i < workerCount; i++) {
      workers[i] = CompletableFuture.runAsync(() -> {
        Runnable task;
        while ((task = taskQueue.poll()) != null) {
          task.run();
        }
      }, executor);
    }
    CompletableFuture.allOf(workers).join();
  }

  private record StreamSubscriptionState(String eventType, Object metadata) {
  }

  /** 24hr fallback entry: price/volume statistics and full-ticker-only fields each keep their own time */
  private record Ticker24HrEntry(Ticker24Hr ticker, long statsTime, long fullTime) {
  }

  /** all-market ticker streams: each frame carries only the symbols that changed in the last second */
  protected enum AllMarketTickerStream {
    TICKER("!ticker@arr", "24hrTicker"),
    MINI_TICKER("!miniTicker@arr", "24hrMiniTicker");

    private final String topic;

    private final String eventType;

    AllMarketTickerStream(String topic, String eventType) {
      this.topic = topic;
      this.eventType = eventType;
    }

    public String topic() {
      return topic;
    }

    public String eventType() {
      return eventType;
    }
  }

  private record ExpectedTopicsSnapshot(Set<String> symbols,
                                        Set<String> subscribeIntervals,
                                        Set<String> extraTopics,
                                        Set<String> tickerTopics,
                                        Map<String, StreamSubscriptionState> subscriptionState,
                                        Set<String> expectedTopics) {

    private boolean matches(Set<String> currentSymbols,
                            Set<String> currentSubscribeIntervals,
                            Set<String> currentExtraTopics,
                            Set<String> currentTickerTopics,
                            Map<String, StreamSubscriptionState> currentSubscriptionState) {
      return Objects.equals(symbols, currentSymbols)
          && Objects.equals(subscribeIntervals, currentSubscribeIntervals)
          && Objects.equals(extraTopics, currentExtraTopics)
          && Objects.equals(tickerTopics, currentTickerTopics)
          && Objects.equals(subscriptionState, currentSubscriptionState);
    }
  }

  /** @param streamMark latest stream event time when the snapshot was requested */
  private record AllMarketSnapshot<E>(List<E> values, long refreshedAt, long streamMark) {

    private AllMarketSnapshot(List<E> values, long refreshedAt) {
      this(values, refreshedAt, 0L);
    }

    private boolean isStale(long now) {
      return now - refreshedAt >= ALL_MARKET_TICKER_CACHE_TTL.toMillis();
    }
  }

  private boolean allEventTypesMatch(JsonNode eventArrayNode, String eventType) {
    for (JsonNode eventNode : eventArrayNode) {
      if (!StringUtils.equals(extractTextField(eventNode, "e"), eventType)) {
        return false;
      }
    }
    return true;
  }

  private String extractTextField(JsonNode jsonNode, String fieldName) {
    if (jsonNode == null || !jsonNode.isObject()) {
      return null;
    }
    JsonNode fieldNode = jsonNode.get(fieldName);
    if (fieldNode == null || fieldNode.isNull()) {
      return null;
    }
    return fieldNode.asText();
  }

  private static ExecutorService buildKlineFetchExecutor() {
    ThreadFactory namedThreadFactory = ThreadFactoryUtil.getNamedThreadFactory(
        KLINE_FETCH_EXECUTOR_GROUP);
    return new ThreadPoolExecutor(0, 20,
        10, TimeUnit.MINUTES, new SynchronousQueue<>(),
        namedThreadFactory, new CallerRunsPolicy());
  }

  private static ExecutorService buildManageExecutor() {
    ThreadFactory namedThreadFactory = ThreadFactoryUtil.getNamedThreadFactory(
        MANAGE_EXECUTOR_GROUP);
    return new ThreadPoolExecutor(
        5,
        20,
        1,
        TimeUnit.MINUTES,
        new ArrayBlockingQueue<>(1024),
        namedThreadFactory,
        new AbortPolicy());
  }

  private NumberTypeEnum getNumberType() {
    return CommonUtil.getEnumByCode(numberType, NumberTypeEnum.class);
  }

  private boolean isPersistenceEnabled() {
    return persistenceProperties != null && persistenceProperties.isEnabled();
  }

  /**
   * only intervals explicitly listed under kline.persistence.&lt;service&gt;.intervalConfigs
   * are persisted. Day-sharded storage writes ONE file per day per symbol, so persisting
   * a "1d" series would explode into (maintainCount) tiny files per symbol — an interval
   * must opt in deliberately.
   */
  private boolean isPersistenceEnabledFor(IntervalEnum intervalEnum) {
    if (!isPersistenceEnabled() || intervalEnum == null) {
      return false;
    }
    return getPersistenceServiceConfig().getIntervalConfigs().containsKey(intervalEnum.code());
  }

  private void startPersistedKlineWarmup(Set<KlineSetKey> restoredKlineSetKeys,
                                         Set<KlineSetKey> configuredKlineSetKeys) {
    if (!isPersistenceEnabled() || CollectionUtils.isEmpty(restoredKlineSetKeys)
        && CollectionUtils.isEmpty(configuredKlineSetKeys)) {
      return;
    }
    CompletableFuture.runAsync(() -> {
      warmUpPersistedKlines(restoredKlineSetKeys);
      reconcilePersistedKlines(configuredKlineSetKeys);
    }, MANAGE_EXECUTOR);
  }

  private Set<KlineSetKey> restorePersistedKlines(Set<KlineSetKey> configuredKlineSetKeys) {
    if (!isPersistenceEnabled() || !persistenceProperties.isLoadOnStartup()
        || CollectionUtils.isEmpty(configuredKlineSetKeys)) {
      return Set.of();
    }
    long restoreStartTime = System.currentTimeMillis();
    Set<KlineSetKey> restoredKeys = ConcurrentHashMap.newKeySet();
    List<Runnable> restoreTasks = configuredKlineSetKeys.stream()
        .<Runnable>map(configuredKey -> () -> {
          IntervalEnum intervalEnum = CommonUtil.getEnumByCode(configuredKey.getInterval(), IntervalEnum.class);
          if (!isPersistenceEnabledFor(intervalEnum)) {
            return;
          }
          // best-effort cache: one broken symbol dir must not abort startup for the rest
          try {
            // startup only needs the serving window; deeper disk data would just slow down boot
            int restoreCount = getPersistenceRestoreCount(configuredKey.getSymbol(), intervalEnum);
            List<PersistedKlineRow> persistedRows = klinePersistenceStore.loadRows(
                getPersistenceServiceCode(), configuredKey.getInterval(), configuredKey.getSymbol(), restoreCount);
            if (CollectionUtils.isEmpty(persistedRows)) {
              return;
            }
            List<Kline> persistedKlines = persistedRows.stream()
                .map(this::persistedRowToKline)
                .filter(Objects::nonNull)
                .toList();
            if (CollectionUtils.isEmpty(persistedKlines)) {
              return;
            }
            restoreKlines(configuredKey, intervalEnum, persistedKlines);
            restoredKeys.add(configuredKey);
          } catch (Exception e) {
            log.warn("failed to restore persisted klines for symbol: {}, interval: {}, skipped.",
                configuredKey.getSymbol(), configuredKey.getInterval(), e);
          }
        }).toList();
    // dedicated pool: MANAGE_EXECUTOR only runs 5 core threads (queue absorbs the rest),
    // which would cap restore parallelism regardless of RESTORE_WORKERS
    ExecutorService restoreExecutor = Executors.newFixedThreadPool(
        Math.max(1, Math.min(RESTORE_WORKERS, restoreTasks.size())),
        ThreadFactoryUtil.getNamedThreadFactory("kline-restore"));
    try {
      runTasksWithLimitedWorkers(restoreTasks, RESTORE_WORKERS, restoreExecutor);
    } finally {
      restoreExecutor.shutdown();
    }
    if (CollectionUtils.isNotEmpty(restoredKeys)) {
      log.info("restored {} persisted kline series for {} in {} ms",
          restoredKeys.size(), getClass().getSimpleName(), System.currentTimeMillis() - restoreStartTime);
    }
    return restoredKeys;
  }

  private void warmUpPersistedKlines(Set<KlineSetKey> restoredKlineSetKeys) {
    if (CollectionUtils.isEmpty(restoredKlineSetKeys)) {
      return;
    }
    List<Runnable> warmupTasks = restoredKlineSetKeys.stream()
        .<Runnable>map(klineSetKey -> () -> {
          IntervalEnum intervalEnum = CommonUtil.getEnumByCode(klineSetKey.getInterval(), IntervalEnum.class);
          if (intervalEnum == null) {
            return;
          }
          // warm up the SAME window the restore loaded — a wider window would re-fetch
          // bars from Binance that are already on disk but deliberately not loaded
          int restoreCount = getPersistenceRestoreCount(klineSetKey.getSymbol(), intervalEnum);
          List<ImmutablePair<Long, Long>> makeUpTimeRanges = buildMakeUpTimeRanges(
              klineSetKey.getSymbol(), null, getServerTime(), intervalEnum, restoreCount, true);
          if (CollectionUtils.isEmpty(makeUpTimeRanges)) {
            return;
          }
          fetchAndStoreKlines(klineSetKey.getSymbol(), klineSetKey.getInterval(),
              filterMakeUpTimeRangesByOnboardTime(klineSetKey.getSymbol(), makeUpTimeRanges), 1, MANAGE_EXECUTOR);
        }).toList();
    runTasksWithLimitedWorkers(warmupTasks, MAX_RPC_SYNC_WORKERS, MANAGE_EXECUTOR);
  }

  private void reconcilePersistedKlines(Set<KlineSetKey> configuredKlineSetKeys) {
    if (CollectionUtils.isEmpty(configuredKlineSetKeys)) {
      return;
    }
    long currentTime = getServerTime();
    for (KlineSetKey klineSetKey : configuredKlineSetKeys) {
      // runs inside the async warmup future: one failing key must not silently kill the rest
      try {
        dumpPersistedKlineSetTracked(klineSetKey, currentTime);
      } catch (Exception e) {
        log.warn("failed to reconcile persisted klines for symbol: {}, interval: {}, skipped.",
            klineSetKey.getSymbol(), klineSetKey.getInterval(), e);
      }
    }
  }

  private void dumpPersistedKlines(boolean force) {
    if (!isPersistenceEnabled()) {
      return;
    }
    Set<KlineSetKey> keys = force ? new HashSet<>(klineSetMap.keySet()) : new HashSet<>(dirtyPersistenceKeys);
    if (CollectionUtils.isEmpty(keys)) {
      return;
    }
    long currentTime = getServerTime();
    for (KlineSetKey key : keys) {
      // one failing symbol (e.g. transient IO error) must not skip the remaining dumps
      try {
        dumpPersistedKlineSetTracked(key, currentTime);
      } catch (Exception e) {
        log.warn("failed to dump persisted klines for symbol: {}, interval: {}, skipped.",
            key.getSymbol(), key.getInterval(), e);
      }
    }
  }

  /**
   * remove-before-dump protocol: clearing the dirty flag AFTER the dump can erase a dirty
   * signal raised by a concurrent updateKlines during the write. Removing it first means a
   * concurrent update simply re-adds the key and the next cycle retries; on skip/failure
   * the flag is restored.
   */
  private void dumpPersistedKlineSetTracked(KlineSetKey klineSetKey, long currentTime) {
    boolean wasDirty = dirtyPersistenceKeys.remove(klineSetKey);
    boolean dumped;
    try {
      dumped = dumpPersistedKlineSet(klineSetKey, currentTime);
    } catch (Exception e) {
      restoreDirtyFlagIfStillTracked(klineSetKey, wasDirty);
      throw e;
    }
    if (!dumped) {
      restoreDirtyFlagIfStillTracked(klineSetKey, wasDirty);
    }
  }

  private void restoreDirtyFlagIfStillTracked(KlineSetKey klineSetKey, boolean wasDirty) {
    // gate on klineSetMap membership: a key deleted by concurrent offline cleanup must not
    // be re-added, or it would sit in dirtyPersistenceKeys retrying forever
    if (wasDirty && klineSetMap.containsKey(klineSetKey)) {
      dirtyPersistenceKeys.add(klineSetKey);
    }
  }

  /**
   * @return true when the key needs no further dumping (written, or nothing will ever be
   *         persistable for it); false when the dump was SKIPPED and the dirty flag must
   *         survive so the next cycle retries
   */
  private boolean dumpPersistedKlineSet(KlineSetKey klineSetKey, long currentTime) {
    IntervalEnum intervalEnum = CommonUtil.getEnumByCode(klineSetKey.getInterval(), IntervalEnum.class);
    if (!isPersistenceEnabledFor(intervalEnum)) {
      // nothing will ever be persisted for this key: drop any dirty flag
      return true;
    }
    // snapshot INSIDE the lock: a delayed writer must never overwrite a newer snapshot,
    // nor resurrect shards that concurrent offline cleanup just deleted
    synchronized (getPersistenceDumpLock(klineSetKey)) {
      KlineSet klineSet = klineSetMap.get(klineSetKey);
      Collection<Kline> sourceKlines = klineSet == null ? List.of() : klineSet.finalSnapshot(currentTime);
      List<PersistedKlineRow> persistedRows = sourceKlines.stream()
          .sorted(Comparator.comparingLong(Kline::getOpenTime))
          .map(this::toPersistedKlineRow)
          .toList();
      if (CollectionUtils.isEmpty(persistedRows)) {
        // an empty in-memory set (failed restore, loadOnStartup=false, or only the open
        // candle yet) must NOT wipe recoverable shards on disk; explicit deletion happens
        // only through cleanupPersistedKlines when a symbol goes offline
        return false;
      }
      klinePersistenceStore.dumpRows(getPersistenceServiceCode(), klineSetKey.getInterval(),
          klineSetKey.getSymbol(), persistedRows,
          getPersistenceMaxStoreCount(klineSetKey.getSymbol(), intervalEnum), currentTime);
      return true;
    }
  }

  private void cleanupPersistedKlines(KlineSetKey klineSetKey) {
    if (!isPersistenceEnabled()) {
      return;
    }
    IntervalEnum intervalEnum = CommonUtil.getEnumByCode(klineSetKey.getInterval(), IntervalEnum.class);
    if (intervalEnum == null) {
      return;
    }
    synchronized (getPersistenceDumpLock(klineSetKey)) {
      klinePersistenceStore.dumpRows(getPersistenceServiceCode(), klineSetKey.getInterval(),
          klineSetKey.getSymbol(), List.of(),
          getPersistenceMaxStoreCount(klineSetKey.getSymbol(), intervalEnum), getServerTime());
    }
    dirtyPersistenceKeys.remove(klineSetKey);
  }

  private Object getPersistenceDumpLock(KlineSetKey klineSetKey) {
    return persistenceDumpLocks[Math.floorMod(klineSetKey.hashCode(), PERSISTENCE_DUMP_LOCK_STRIPES)];
  }

  private void restoreKlines(KlineSetKey klineSetKey, IntervalEnum intervalEnum, List<Kline> klines) {
    KlineSet klineSet = klineSetMap.computeIfAbsent(klineSetKey, var -> new KlineSet(klineSetKey));
    long now = getServerTime();
    boolean finalRevised = false;
    for (Kline kline : klines) {
      KlineSet.Commit commit = klineSet.commit(kline, kline.getCloseTime() <= now,
          KlineUpdateSource.RESTORE, null, 0L);
      if (commit.becameFinal()) {
        signalFinal(klineSetKey.getSymbol(), klineSetKey.getInterval(), kline.getOpenTime());
      }
      finalRevised |= commit.finalRevised();
    }
    trimKlinesIfNeeded(klineSet, intervalEnum);
    if (finalRevised) {
      finalRevision.incrementAndGet();
      bulkKlinesCache.invalidateAll();
    }
  }

  private PersistedKlineRow toPersistedKlineRow(Kline kline) {
    Object[] displayKline = ConvertUtil.convertToDisplayKline(kline);
    PersistedKlineRow row = new PersistedKlineRow();
    row.setOpenTime(((Number) displayKline[0]).longValue());
    row.setOpenPrice(String.valueOf(displayKline[1]));
    row.setHighPrice(String.valueOf(displayKline[2]));
    row.setLowPrice(String.valueOf(displayKline[3]));
    row.setClosePrice(String.valueOf(displayKline[4]));
    row.setVolume(String.valueOf(displayKline[5]));
    row.setCloseTime(((Number) displayKline[6]).longValue());
    row.setQuoteVolume(String.valueOf(displayKline[7]));
    row.setTradeNum(((Number) displayKline[8]).intValue());
    row.setActiveBuyVolume(String.valueOf(displayKline[9]));
    row.setActiveBuyQuoteVolume(String.valueOf(displayKline[10]));
    return row;
  }

  private Kline persistedRowToKline(PersistedKlineRow row) {
    Object[] serverKline = new Object[] {
        row.getOpenTime(),
        row.getOpenPrice(),
        row.getHighPrice(),
        row.getLowPrice(),
        row.getClosePrice(),
        row.getVolume(),
        row.getCloseTime(),
        row.getQuoteVolume(),
        row.getTradeNum(),
        row.getActiveBuyVolume(),
        row.getActiveBuyQuoteVolume()
    };
    // a single unparseable persisted row must not abort the whole restore
    try {
      return serverKlineToKline(serverKline);
    } catch (Exception e) {
      log.warn("failed to convert persisted kline row, openTime: {}, skipped.", row.getOpenTime(), e);
      return null;
    }
  }

  private Set<KlineSetKey> buildConfiguredKlineSetKeys() {
    List<String> symbols = getSymbols();
    Set<KlineSetKey> configuredKeys = new HashSet<>();
    for (IntervalEnum interval : getSubscribeIntervals()) {
      for (String symbol : getSubscribeSymbols(symbols, interval)) {
        configuredKeys.add(new KlineSetKey(symbol, interval.code()));
      }
    }
    return configuredKeys;
  }

  private int getPersistenceRestoreCount(String symbol, IntervalEnum intervalEnum) {
    IntervalSyncConfig intervalSyncConfig = getSyncConfig().getIntervalSyncConfigs().get(intervalEnum.code());
    Integer minMaintainCount = intervalSyncConfig == null ? null : intervalSyncConfig.getMinMaintainCount();
    if (minMaintainCount != null && minMaintainCount > 0) {
      return minMaintainCount;
    }
    return getPersistenceMaxStoreCount(symbol, intervalEnum);
  }

  private int getPersistenceMaxStoreCount(String symbol, IntervalEnum intervalEnum) {
    IntervalSyncConfig intervalSyncConfig = getSyncConfig().getIntervalSyncConfigs().get(intervalEnum.code());
    int defaultMaxStoreCount = intervalSyncConfig.getMinMaintainCount() * 2;
    if (!isPersistenceEnabled()) {
      return defaultMaxStoreCount;
    }
    KlinePersistenceProperties.ServicePersistenceConfig servicePersistenceConfig = getPersistenceServiceConfig();
    KlinePersistenceProperties.IntervalPersistenceConfig intervalPersistenceConfig =
        servicePersistenceConfig.getIntervalConfigs().get(intervalEnum.code());
    Integer symbolMaxStoreCount = intervalPersistenceConfig == null
        ? null
        : intervalPersistenceConfig.getSymbolMaxStoreCounts().get(symbol);
    Integer intervalMaxStoreCount = intervalPersistenceConfig == null ? null : intervalPersistenceConfig.getMaxStoreCount();
    int configuredMaxStoreCount = symbolMaxStoreCount != null
        ? symbolMaxStoreCount
        : intervalMaxStoreCount != null ? intervalMaxStoreCount : defaultMaxStoreCount;
    return Math.max(configuredMaxStoreCount, intervalSyncConfig.getMinMaintainCount());
  }

  private int getEffectiveMaintainCount(String symbol, IntervalEnum intervalEnum) {
    IntervalSyncConfig intervalSyncConfig = getSyncConfig().getIntervalSyncConfigs().get(intervalEnum.code());
    if (intervalSyncConfig == null || intervalSyncConfig.getMinMaintainCount() == null) {
      return 0;
    }
    if (!isPersistenceEnabled()) {
      return intervalSyncConfig.getMinMaintainCount();
    }
    return Math.max(intervalSyncConfig.getMinMaintainCount(), getPersistenceMaxStoreCount(symbol, intervalEnum));
  }

  private int getBackgroundRefreshKlineCount(String symbol, IntervalEnum intervalEnum) {
    int refreshKlineCount = getEffectiveMaintainCount(symbol, intervalEnum);
    Integer rpcRefreshCount = getSyncConfig().getRpcRefreshCount();
    int realRpcRefreshCount = rpcRefreshCount == null ? refreshKlineCount : rpcRefreshCount;
    KlineSetKey key = new KlineSetKey(symbol, intervalEnum.code());
    KlineSet klineSet = klineSetMap.get(key);
    if (klineSet != null && getMapSize(klineSet.getKlineMap(), intervalEnum) >= realRpcRefreshCount) {
      return realRpcRefreshCount;
    }
    return refreshKlineCount;
  }

  private KlinePersistenceProperties.ServicePersistenceConfig getPersistenceServiceConfig() {
    if (StringUtils.equals(getPersistenceServiceCode(), "future")) {
      return persistenceProperties.getFuture();
    }
    return persistenceProperties.getSpot();
  }

  private int getMapSize(NavigableMap<Long, ?> map, IntervalEnum interval) {
    if (MapUtils.isEmpty(map)) {
      return 0;
    }
    if (map.firstKey() == null || map.lastKey() == null) {
      return 1;
    }
    return (int) (((map.lastKey() - map.firstKey()) / interval.getMills()) + 1);
  }
}
