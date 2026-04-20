package com.zx.quant.klineproxy.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.model.EventKline;
import com.zx.quant.klineproxy.model.EventKline.BigDecimalEventKline;
import com.zx.quant.klineproxy.model.EventKline.DoubleEventKline;
import com.zx.quant.klineproxy.model.EventKline.FloatEventKline;
import com.zx.quant.klineproxy.model.EventKline.StringEventKline;
import com.zx.quant.klineproxy.model.EventKlineEvent;
import com.zx.quant.klineproxy.model.EventTicker24HrEvent;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Kline.BigDecimalKline;
import com.zx.quant.klineproxy.model.Kline.DoubleKline;
import com.zx.quant.klineproxy.model.Kline.FloatKline;
import com.zx.quant.klineproxy.model.Kline.StringKline;
import com.zx.quant.klineproxy.model.KlineSet;
import com.zx.quant.klineproxy.model.KlineSetKey;
import com.zx.quant.klineproxy.model.ParsedWebSocketMessage;
import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.model.Ticker24Hr;
import com.zx.quant.klineproxy.model.config.KlinePersistenceProperties;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties.IntervalSyncConfig;
import com.zx.quant.klineproxy.model.enums.IntervalEnum;
import com.zx.quant.klineproxy.model.enums.NumberTypeEnum;
import com.zx.quant.klineproxy.model.exceptions.ApiException;
import com.zx.quant.klineproxy.model.persistence.PersistedKlineRow;
import com.zx.quant.klineproxy.monitor.MonitorManager;
import com.zx.quant.klineproxy.service.KlinePersistenceStore;
import com.zx.quant.klineproxy.service.KlineService;
import com.zx.quant.klineproxy.util.CommonUtil;
import com.zx.quant.klineproxy.util.ConvertUtil;
import com.zx.quant.klineproxy.util.ExceptionSafeRunnable;
import com.zx.quant.klineproxy.util.Serializer;
import com.zx.quant.klineproxy.util.ThreadFactoryUtil;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import jakarta.annotation.PreDestroy;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
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

  private static final IntervalEnum DEFAULT_TICKER_INTERVAL = IntervalEnum.ONE_HOUR;

  private static final String KLINE_EVENT = "kline";

  private static final String TICKER_24HR_EVENT = "24hrTicker";

  private static final String TICKER_24HR_TOPIC = "!ticker@arr";

  private static final int SYMBOLS_PER_CONNECTION = 150;

  private static final int MAX_RPC_SYNC_WORKERS = 8;

  private static final int MAX_MAKE_UP_WORKERS = 4;

  private static final int KLINE_TRIM_BUFFER = 50;

  private static final long SUBSCRIBE_TOPICS_ADJUST_INTERVAL_MILLS = 30_000L;

  private static final Duration ALL_MARKET_TICKER_CACHE_TTL = Duration.ofMillis(500);

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

  private final Cache<String, Ticker24Hr> ticker24HrCache = Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofDays(1))
      .build();

  private final AtomicReference<AllMarketSnapshot<Ticker<?>>> allMarketTickerSnapshot =
      new AtomicReference<>(new AllMarketSnapshot<>(List.of(), 0L));

  private final AtomicReference<AllMarketSnapshot<Ticker24Hr>> allMarketTicker24HrSnapshot =
      new AtomicReference<>(new AllMarketSnapshot<>(List.of(), 0L));

  private final AtomicBoolean refreshingAllMarketTickerSnapshot = new AtomicBoolean(false);

  private final AtomicBoolean refreshingAllMarketTicker24HrSnapshot = new AtomicBoolean(false);

  private final AtomicLong lastAllMarketTickerAccessTime = new AtomicLong(0L);

  private final AtomicLong lastAllMarketTicker24HrAccessTime = new AtomicLong(0L);

  private final Set<KlineSetKey> dirtyPersistenceKeys = ConcurrentHashMap.newKeySet();

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

  protected abstract KlineSyncConfigProperties getSyncConfig();

  protected abstract int getMakeUpKlinesLimit();

  protected abstract int getMakeUpKlinesWeight();

  protected abstract int getTicker24HrsWeight();

  protected abstract String getServiceType();

  protected abstract String getPersistenceServiceCode();

  protected long getServerTime() {
    return System.currentTimeMillis();
  }

  protected Collection<String> getTicker24HrSubscribeTopics() {
    return Set.of(TICKER_24HR_TOPIC);
  }

  protected String getTicker24HrStreamTopic(String symbol) {
    return TICKER_24HR_TOPIC;
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

  @Override
  public List<Ticker<?>> queryTickers(Collection<String> symbols) {
    if (CollectionUtils.isNotEmpty(symbols)) {
      List<Ticker<?>> realtimeTickers = queryTickersBySymbols(symbols);
      if (CollectionUtils.isNotEmpty(realtimeTickers)) {
        return realtimeTickers;
      }
    } else {
      List<Ticker<?>> realtimeTickers = queryAllMarketTickersSnapshot();
      if (CollectionUtils.isNotEmpty(realtimeTickers)) {
        return realtimeTickers;
      }
    }
    IntervalEnum intervalEnum = DEFAULT_TICKER_INTERVAL;
    List<IntervalEnum> subscribeIntervals = getSubscribeIntervals();
    if (CollectionUtils.isNotEmpty(subscribeIntervals)) {
      intervalEnum = subscribeIntervals.get(0);
    }
    String interval = intervalEnum.code();
    long serverTime = getServerTime();
    List<Ticker<?>> tickers = new ArrayList<>();
    Collection<String> realSymbols;
    if (CollectionUtils.isNotEmpty(symbols)) {
      realSymbols = symbols;
    } else {
      realSymbols = klineSetMap.keySet().stream()
          .filter(key -> StringUtils.equals(key.getInterval(), interval))
          .map(KlineSetKey::getSymbol)
          .toList();
    }

    for (String symbol : realSymbols) {
      KlineSetKey key = new KlineSetKey(symbol, interval);
      KlineSet klineSet = klineSetMap.computeIfAbsent(key, var -> new KlineSet(key));
      NavigableMap<Long, Kline> klineMap = klineSet.getKlineMap();
      if (MapUtils.isNotEmpty(klineMap)) {
        Kline kline = klineMap.lastEntry().getValue();
        Ticker<?> ticker = Ticker.create(symbol, kline, serverTime);
        tickers.add(ticker);
      }
    }
    return tickers;
  }

  @Override
  public List<Ticker24Hr> queryTicker24hrs(Collection<String> symbols) {
    if (CollectionUtils.isNotEmpty(symbols)) {
      List<Ticker24Hr> realtimeTicker24Hrs = queryTicker24HrsBySymbols(symbols);
      if (CollectionUtils.isNotEmpty(realtimeTicker24Hrs)) {
        ticker24HrCache.putAll(realtimeTicker24Hrs.stream()
            .collect(Collectors.toMap(Ticker24Hr::getSymbol, Function.identity(), (o, n) -> n)));
        return realtimeTicker24Hrs;
      }
    }
    if (CollectionUtils.isEmpty(symbols)) {
      List<Ticker24Hr> realtimeTicker24Hrs = queryAllMarketTicker24HrsSnapshot();
      if (CollectionUtils.isNotEmpty(realtimeTicker24Hrs)) {
        return realtimeTicker24Hrs;
      }
      return List.copyOf(ticker24HrCache.asMap().values());
    }
    return symbols.stream()
        .map(ticker24HrCache::getIfPresent)
        .filter(Objects::nonNull)
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
  public void updateKlines(String symbol, String interval, List<Kline> klines) {
    if (CollectionUtils.isEmpty(klines) || StringUtils.isBlank(symbol) || StringUtils.isBlank(interval)) {
      return;
    }
    IntervalEnum intervalEnum = CommonUtil.getEnumByCode(interval, IntervalEnum.class);
    KlineSetKey klineSetKey = new KlineSetKey(symbol, interval);
    KlineSet klineSet = klineSetMap.computeIfAbsent(klineSetKey, var -> new KlineSet(klineSetKey));
    NavigableMap<Long, Kline> klineMap = klineSet.getKlineMap();
    Entry<Long, Kline> existLastEntry = klineMap.lastEntry();
    List<Kline> klinesToUpdate = new ArrayList<>();
    if (existLastEntry != null) {
      klinesToUpdate.add(existLastEntry.getValue());
    }
    klinesToUpdate.addAll(klines);
    klinesToUpdate = fillKlines(klinesToUpdate, intervalEnum);
    boolean updated = false;
    for (Kline kline : klinesToUpdate) {
      Kline existKline = klineMap.get(kline.getOpenTime());
      if (existKline == null || existKline.getTradeNum() < kline.getTradeNum()) {
        klineMap.put(kline.getOpenTime(), kline);
        updated = true;
      }
    }
    trimKlinesIfNeeded(klineSet, intervalEnum);
    if (updated && isPersistenceEnabled()) {
      dirtyPersistenceKeys.add(klineSetKey);
    }
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
    return parsedMessage -> doForKlineEvent(convertToEventKlineEvent(parsedMessage));
  }

  protected Function<ParsedWebSocketMessage, Boolean> getTicker24HrEventMessageHandler() {
    return parsedMessage -> doForTicker24HrEvents(convertToEventTicker24HrEvent(parsedMessage));
  }

  protected boolean doForKlineEvent(EventKlineEvent<?, ?> eventKlineEvent) {
    if (eventKlineEvent == null || !StringUtils.equals(eventKlineEvent.getEventType(), KLINE_EVENT)) {
      return false;
    }
    Kline kline = convertToKline(eventKlineEvent);
    String symbol = eventKlineEvent.getSymbol();
    String interval = eventKlineEvent.getEventKline().getInterval();
    updateKline(symbol, interval, kline);
    monitorManager.incReceivedKlineMessage(getServiceType(), interval, symbol);
    return true;
  }

  protected boolean doForTicker24HrEvents(List<EventTicker24HrEvent> eventTicker24HrEvents) {
    if (CollectionUtils.isEmpty(eventTicker24HrEvents)) {
      return false;
    }
    Optional<EventTicker24HrEvent> invalidEventOptional = eventTicker24HrEvents.stream()
        .filter(event -> !StringUtils.equals(event.getEventType(), TICKER_24HR_EVENT))
        .findAny();
    if (invalidEventOptional.isPresent()) {
      return false;
    }
    eventTicker24HrEvents.forEach(this::doForTicker24HrEvent);
    return true;
  }

  protected void doForTicker24HrEvent(EventTicker24HrEvent eventTicker24HrEvent) {
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

    ticker24HrCache.put(ticker24Hr.getSymbol(), ticker24Hr);
    monitorManager.incReceivedTickerMessage(getServiceType());
  }

  protected Function<ParsedWebSocketMessage, String> getKlineEventMessageTopicExtractor() {
    return parsedMessage -> {
      if (!StringUtils.equals(parsedMessage.eventType(), KLINE_EVENT)) {
        return null;
      }
      if (StringUtils.isNotBlank(parsedMessage.stream())) {
        return parsedMessage.stream();
      }
      JsonNode payloadNode = parsedMessage.payloadNode();
      String symbol = extractTextField(payloadNode, "s");
      String interval = extractTextField(payloadNode != null ? payloadNode.get("k") : null, "i");
      if (StringUtils.isAnyBlank(symbol, interval)) {
        return null;
      }
      return buildSymbolUpdateTopic(symbol, interval);
    };
  }

  protected Function<ParsedWebSocketMessage, String> getTicker24HrEventMessageTopicExtractor() {
    return parsedMessage -> {
      JsonNode payloadNode = parsedMessage.payloadNode();
      if (payloadNode == null || payloadNode.isNull()) {
        return null;
      }
      if (payloadNode.isArray()) {
        if (payloadNode.isEmpty() || !allEventTypesMatch(payloadNode, TICKER_24HR_EVENT)) {
          return null;
        }
        return TICKER_24HR_TOPIC;
      }
      if (!StringUtils.equals(parsedMessage.eventType(), TICKER_24HR_EVENT)) {
        return null;
      }
      if (StringUtils.isNotBlank(parsedMessage.stream())) {
        return parsedMessage.stream();
      }
      String symbol = extractTextField(payloadNode, "s");
      if (StringUtils.isBlank(symbol)) {
        return null;
      }
      return getTicker24HrStreamTopic(symbol);
    };
  }

  protected String buildSymbolUpdateTopic(String symbol, String interval) {
    return StringUtils.lowerCase(symbol) + "@kline_" + interval;
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
    SCHEDULE_EXECUTOR_SERVICE.scheduleWithFixedDelay(
        new ExceptionSafeRunnable(() -> {
          long now = System.currentTimeMillis();
          refreshAllMarketSnapshotIfActive(now, lastAllMarketTickerAccessTime, allMarketTickerSnapshot,
              this::triggerAllMarketTickerRefresh);
          refreshAllMarketSnapshotIfActive(now, lastAllMarketTicker24HrAccessTime, allMarketTicker24HrSnapshot,
              this::triggerAllMarketTicker24HrRefresh);
        }), 1000, ALL_MARKET_SNAPSHOT_REFRESH_CHECK_INTERVAL_MILLS, TimeUnit.MILLISECONDS);
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

  private void triggerAllMarketTickerRefreshIfStale(AllMarketSnapshot<Ticker<?>> snapshot) {
    if (snapshot.isStale(System.currentTimeMillis())) {
      triggerAllMarketTickerRefresh();
    }
  }

  private void triggerAllMarketTicker24HrRefreshIfStale(AllMarketSnapshot<Ticker24Hr> snapshot) {
    if (snapshot.isStale(System.currentTimeMillis())) {
      triggerAllMarketTicker24HrRefresh();
    }
  }

  private void triggerAllMarketTickerRefresh() {
    triggerAllMarketSnapshotRefresh(refreshingAllMarketTickerSnapshot,
        this::loadAllMarketTickerSnapshot,
        allMarketTickerSnapshot::set);
  }

  private void triggerAllMarketTicker24HrRefresh() {
    triggerAllMarketSnapshotRefresh(refreshingAllMarketTicker24HrSnapshot,
        this::loadAllMarketTicker24HrSnapshot,
        snapshot -> {
          allMarketTicker24HrSnapshot.set(snapshot);
          syncTicker24HrCache(snapshot.values());
        });
  }

  private <E> void triggerAllMarketSnapshotRefresh(AtomicBoolean refreshing,
                                                   Supplier<AllMarketSnapshot<E>> loader,
                                                   Consumer<AllMarketSnapshot<E>> snapshotUpdater) {
    if (!refreshing.compareAndSet(false, true)) {
      return;
    }
    CompletableFuture.runAsync(() -> {
      try {
        snapshotUpdater.accept(loader.get());
      } finally {
        refreshing.set(false);
      }
    }, MANAGE_EXECUTOR);
  }

  private List<Ticker<?>> refreshAllMarketTickersSnapshotNow() {
    AllMarketSnapshot<Ticker<?>> snapshot = loadAllMarketTickerSnapshot();
    allMarketTickerSnapshot.set(snapshot);
    return snapshot.values();
  }

  private List<Ticker24Hr> refreshAllMarketTicker24HrsSnapshotNow() {
    AllMarketSnapshot<Ticker24Hr> snapshot = loadAllMarketTicker24HrSnapshot();
    allMarketTicker24HrSnapshot.set(snapshot);
    syncTicker24HrCache(snapshot.values());
    return snapshot.values();
  }

  private AllMarketSnapshot<Ticker<?>> loadAllMarketTickerSnapshot() {
    List<Ticker<?>> realtimeTickers = queryTickers0();
    if (CollectionUtils.isEmpty(realtimeTickers)) {
      return new AllMarketSnapshot<>(List.of(), System.currentTimeMillis());
    }
    return new AllMarketSnapshot<>(List.copyOf(realtimeTickers), System.currentTimeMillis());
  }

  private AllMarketSnapshot<Ticker24Hr> loadAllMarketTicker24HrSnapshot() {
    List<Ticker24Hr> realtimeTicker24Hrs = queryTicker24Hrs();
    if (CollectionUtils.isEmpty(realtimeTicker24Hrs)) {
      return new AllMarketSnapshot<>(List.of(), System.currentTimeMillis());
    }
    return new AllMarketSnapshot<>(List.copyOf(realtimeTicker24Hrs), System.currentTimeMillis());
  }

  private void syncTicker24HrCache(List<Ticker24Hr> ticker24Hrs) {
    if (CollectionUtils.isEmpty(ticker24Hrs)) {
      return;
    }
    Map<String, Ticker24Hr> ticker24HrMap = ticker24Hrs.stream()
        .collect(Collectors.toMap(Ticker24Hr::getSymbol, Function.identity(), (o, n) -> n));
    Set<String> needRemoveKeys = ticker24HrCache.asMap().keySet().stream()
        .filter(existKey -> !ticker24HrMap.containsKey(existKey))
        .collect(Collectors.toSet());
    ticker24HrCache.putAll(ticker24HrMap);
    if (CollectionUtils.isNotEmpty(needRemoveKeys)) {
      ticker24HrCache.invalidateAll(needRemoveKeys);
    }
  }

  private List<Ticker<?>> queryAllMarketTickersSnapshot() {
    lastAllMarketTickerAccessTime.set(System.currentTimeMillis());
    AllMarketSnapshot<Ticker<?>> snapshot = allMarketTickerSnapshot.get();
    if (CollectionUtils.isNotEmpty(snapshot.values())) {
      triggerAllMarketTickerRefreshIfStale(snapshot);
      return snapshot.values();
    }
    return refreshAllMarketTickersSnapshotNow();
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
    SCHEDULE_EXECUTOR_SERVICE.scheduleWithFixedDelay(
        new ExceptionSafeRunnable(() -> {
          List<Ticker24Hr> ticker24Hrs = queryTicker24Hrs();
          if (CollectionUtils.isEmpty(ticker24Hrs)) {
            return;
          }
          AllMarketSnapshot<Ticker24Hr> snapshot = new AllMarketSnapshot<>(List.copyOf(ticker24Hrs),
              System.currentTimeMillis());
          allMarketTicker24HrSnapshot.set(snapshot);
          syncTicker24HrCache(snapshot.values());
        }), 1000, 1000 * 60, TimeUnit.MILLISECONDS
    );
  }

  private void startKlineRpcUpdater() {
    SCHEDULE_EXECUTOR_SERVICE.scheduleWithFixedDelay(
        new ExceptionSafeRunnable(this::syncConfiguredKlinesOnce), 1000, 1000 * 60 * 5, TimeUnit.MILLISECONDS);
  }

  private void syncConfiguredKlinesOnce() {
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
        String topic = buildSymbolUpdateTopic(symbol, interval);
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
    ExpectedTopicsSnapshot snapshot = expectedTopicsSnapshot;
    if (snapshot != null && snapshot.matches(symbols, subscribeIntervals, extraTopics, tickerTopics)) {
      return snapshot.expectedTopics();
    }

    Set<String> expectTopics = new HashSet<>(extraTopics);
    expectTopics.addAll(tickerTopics);
    expectTopics.addAll(buildNeedSubscribeKlineUpdateTopics(subscribeIntervals, symbols));
    Set<String> immutableExpectedTopics = Set.copyOf(expectTopics);
    expectedTopicsSnapshot = new ExpectedTopicsSnapshot(Set.copyOf(symbols),
        Set.copyOf(subscribeIntervals), extraTopics, tickerTopics, immutableExpectedTopics);
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

  private EventKlineEvent<?, ?> convertToEventKlineEvent(ParsedWebSocketMessage parsedMessage) {
    NumberTypeEnum numberTypeEnum = getNumberType();
    return serializer.treeToValue(parsedMessage.payloadNode(), numberTypeEnum.eventKlineEventClass());
  }

  private List<EventTicker24HrEvent> convertToEventTicker24HrEvent(ParsedWebSocketMessage parsedMessage) {
    JsonNode payloadNode = parsedMessage.payloadNode();
    if (payloadNode == null || payloadNode.isNull()) {
      return null;
    }
    if (payloadNode.isArray()) {
      EventTicker24HrEvent[] eventTicker24HrEventArray = serializer.treeToValue(payloadNode, EventTicker24HrEvent[].class);
      if (eventTicker24HrEventArray == null || eventTicker24HrEventArray.length == 0) {
        return null;
      }
      return new ArrayList<>(List.of(eventTicker24HrEventArray));
    }
    EventTicker24HrEvent eventTicker24HrEvent = serializer.treeToValue(payloadNode, EventTicker24HrEvent.class);
    if (eventTicker24HrEvent == null || StringUtils.isBlank(eventTicker24HrEvent.getEventType())) {
      return null;
    }
    return List.of(eventTicker24HrEvent);
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

  private record ExpectedTopicsSnapshot(Set<String> symbols,
                                        Set<String> subscribeIntervals,
                                        Set<String> extraTopics,
                                        Set<String> tickerTopics,
                                        Set<String> expectedTopics) {

    private boolean matches(Set<String> currentSymbols,
                            Set<String> currentSubscribeIntervals,
                            Set<String> currentExtraTopics,
                            Set<String> currentTickerTopics) {
      return Objects.equals(symbols, currentSymbols)
          && Objects.equals(subscribeIntervals, currentSubscribeIntervals)
          && Objects.equals(extraTopics, currentExtraTopics)
          && Objects.equals(tickerTopics, currentTickerTopics);
    }
  }

  private record AllMarketSnapshot<E>(List<E> values, long refreshedAt) {

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
    Set<KlineSetKey> restoredKeys = new HashSet<>();
    for (KlineSetKey configuredKey : configuredKlineSetKeys) {
      IntervalEnum intervalEnum = CommonUtil.getEnumByCode(configuredKey.getInterval(), IntervalEnum.class);
      if (intervalEnum == null) {
        continue;
      }
      int maxStoreCount = getPersistenceMaxStoreCount(configuredKey.getSymbol(), intervalEnum);
      List<PersistedKlineRow> persistedRows = klinePersistenceStore.loadRows(
          getPersistenceServiceCode(), configuredKey.getInterval(), configuredKey.getSymbol(), maxStoreCount);
      if (CollectionUtils.isEmpty(persistedRows)) {
        continue;
      }
      List<Kline> persistedKlines = persistedRows.stream()
          .map(this::persistedRowToKline)
          .filter(Objects::nonNull)
          .toList();
      if (CollectionUtils.isEmpty(persistedKlines)) {
        continue;
      }
      restoreKlines(configuredKey, intervalEnum, persistedKlines);
      restoredKeys.add(configuredKey);
    }
    if (CollectionUtils.isNotEmpty(restoredKeys)) {
      log.info("restored {} persisted kline series for {}", restoredKeys.size(), getClass().getSimpleName());
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
          int maxStoreCount = getPersistenceMaxStoreCount(klineSetKey.getSymbol(), intervalEnum);
          List<ImmutablePair<Long, Long>> makeUpTimeRanges = buildMakeUpTimeRanges(
              klineSetKey.getSymbol(), null, getServerTime(), intervalEnum, maxStoreCount, true);
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
      dumpPersistedKlineSet(klineSetKey, currentTime);
      dirtyPersistenceKeys.remove(klineSetKey);
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
      dumpPersistedKlineSet(key, currentTime);
      dirtyPersistenceKeys.remove(key);
    }
  }

  private void dumpPersistedKlineSet(KlineSetKey klineSetKey, long currentTime) {
    IntervalEnum intervalEnum = CommonUtil.getEnumByCode(klineSetKey.getInterval(), IntervalEnum.class);
    if (intervalEnum == null) {
      return;
    }
    KlineSet klineSet = klineSetMap.get(klineSetKey);
    Collection<Kline> sourceKlines = klineSet == null ? List.of() : klineSet.getKlineMap().values();
    List<PersistedKlineRow> persistedRows = sourceKlines.stream()
        .filter(kline -> isClosedKline(kline, currentTime))
        .sorted(Comparator.comparingLong(Kline::getOpenTime))
        .map(this::toPersistedKlineRow)
        .toList();
    klinePersistenceStore.dumpRows(getPersistenceServiceCode(), klineSetKey.getInterval(),
        klineSetKey.getSymbol(), persistedRows,
        getPersistenceMaxStoreCount(klineSetKey.getSymbol(), intervalEnum), currentTime);
  }

  private void cleanupPersistedKlines(KlineSetKey klineSetKey) {
    if (!isPersistenceEnabled()) {
      return;
    }
    IntervalEnum intervalEnum = CommonUtil.getEnumByCode(klineSetKey.getInterval(), IntervalEnum.class);
    if (intervalEnum == null) {
      return;
    }
    klinePersistenceStore.dumpRows(getPersistenceServiceCode(), klineSetKey.getInterval(),
        klineSetKey.getSymbol(), List.of(),
        getPersistenceMaxStoreCount(klineSetKey.getSymbol(), intervalEnum), getServerTime());
    dirtyPersistenceKeys.remove(klineSetKey);
  }

  private void restoreKlines(KlineSetKey klineSetKey, IntervalEnum intervalEnum, List<Kline> klines) {
    KlineSet klineSet = klineSetMap.computeIfAbsent(klineSetKey, var -> new KlineSet(klineSetKey));
    NavigableMap<Long, Kline> klineMap = klineSet.getKlineMap();
    for (Kline kline : klines) {
      Kline existKline = klineMap.get(kline.getOpenTime());
      if (existKline == null || existKline.getTradeNum() < kline.getTradeNum()) {
        klineMap.put(kline.getOpenTime(), kline);
      }
    }
    trimKlinesIfNeeded(klineSet, intervalEnum);
  }

  private boolean isClosedKline(Kline kline, long currentTime) {
    return kline != null && currentTime > kline.getCloseTime();
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
    return serverKlineToKline(serverKline);
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
