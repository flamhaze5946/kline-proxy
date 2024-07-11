package com.zx.quant.klineproxy.service.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.model.CombineEvent;
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
import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.model.Ticker24Hr;
import com.zx.quant.klineproxy.model.WebsocketResponse;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties;
import com.zx.quant.klineproxy.model.enums.IntervalEnum;
import com.zx.quant.klineproxy.model.enums.NumberTypeEnum;
import com.zx.quant.klineproxy.service.KlineService;
import com.zx.quant.klineproxy.util.CommonUtil;
import com.zx.quant.klineproxy.util.ExceptionSafeRunnable;
import com.zx.quant.klineproxy.util.Serializer;
import com.zx.quant.klineproxy.util.ThreadFactoryUtil;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

  private static final String KLINE_SCHEDULER_GROUP = "symbols-sync";

  private static final ScheduledExecutorService SCHEDULE_EXECUTOR_SERVICE = new ScheduledThreadPoolExecutor(4,
      ThreadFactoryUtil.getNamedThreadFactory(KLINE_SCHEDULER_GROUP));

  protected static final Integer DEFAULT_LIMIT = 500;

  protected static final Integer MIN_LIMIT = 1;

  protected static final Integer MAX_LIMIT = 1000;

  private final AtomicInteger connectionCount = new AtomicInteger(0);

  private final Map<String, Long> symbolOnboardTimeMap = new ConcurrentHashMap<>();

  private final Set<String> extraSubscribeTopics = new HashSet<>();

  private final Cache<String, Ticker24Hr> ticker24HrCache = Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofDays(1))
      .build();

  @Autowired
  protected RateLimitManager rateLimitManager;

  @Autowired
  protected Serializer serializer;

  @Autowired
  private List<T> webSocketClients = new ArrayList<>();

  @Value("${number.type:bigDecimal}")
  private String numberType;

  private final LoadingCache<String, Optional<CombineEvent>> combineMessageEventLruCache = Caffeine.newBuilder()
      .expireAfterWrite(1, TimeUnit.MINUTES)
      .maximumSize(10240)
      .build(message -> {
        CombineEvent combineEvent = serializer.fromJsonString(message, CombineEvent.class);
        return Optional.ofNullable(combineEvent);
      });

  private final LoadingCache<String, Optional<EventKlineEvent<?, ?>>> messageKlineEventLruCache = Caffeine.newBuilder()
      .expireAfterWrite(1, TimeUnit.MINUTES)
      .maximumSize(10240)
      .build(message -> {
        NumberTypeEnum numberTypeEnum = getNumberType();
        EventKlineEvent<?, ?> eventKlineEvent = serializer.fromJsonString(message, numberTypeEnum.eventKlineEventClass());
        return Optional.ofNullable(eventKlineEvent);
      });

  private final LoadingCache<String, Optional<List<EventTicker24HrEvent>>> messageTicker24HrEventLruCache = Caffeine.newBuilder()
      .expireAfterWrite(1, TimeUnit.MINUTES)
      .maximumSize(10240)
      .build(message -> {
        EventTicker24HrEvent[] eventTicker24HrEventArray = serializer.fromJsonString(message, EventTicker24HrEvent[].class);
        if (eventTicker24HrEventArray == null || eventTicker24HrEventArray.length <= 0) {
          return Optional.empty();
        }
        List<EventTicker24HrEvent> eventTicker24HrEvents = new ArrayList<>(List.of(eventTicker24HrEventArray));
        return Optional.of(eventTicker24HrEvents);
      });

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

  protected long getServerTime() {
    return System.currentTimeMillis();
  }

  @Override
  public List<Ticker<?>> queryTickers(Collection<String> symbols) {
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
    if (CollectionUtils.isEmpty(symbols)) {
      return ticker24HrCache.asMap().values().stream()
          .toList();
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
      throw new RuntimeException("invalid interval");
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
        CompletableFuture<?>[] futures = new CompletableFuture[makeUpTimeRanges.size()];
        for(int i = 0; i < makeUpTimeRanges.size(); i++) {
          ImmutablePair<Long, Long> makeUpRange = makeUpTimeRanges.get(i);
          CompletableFuture<?> makeUpFuture = CompletableFuture.runAsync(() ->
                  safeQueryKlines(symbol, interval,
                      makeUpRange.getLeft(), makeUpRange.getRight(), getMakeUpKlinesLimit())
              , MANAGE_EXECUTOR);
          futures[i] = makeUpFuture;
        }
        CompletableFuture.allOf(futures).join();
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
    KlineSetKey klineSetKey = new KlineSetKey(symbol, interval);
    KlineSet klineSet = klineSetMap.computeIfAbsent(klineSetKey, var -> new KlineSet(klineSetKey));
    NavigableMap<Long, Kline> klineMap = klineSet.getKlineMap();
    for (Kline kline : klines) {
      Kline existKline = klineMap.get(kline.getOpenTime());
      if (existKline == null || existKline.getTradeNum() < kline.getTradeNum()) {
        klineMap.put(kline.getOpenTime(), kline);
      }
    }
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    start();
  }

  protected List<IntervalEnum> getSubscribeIntervals() {
    return getSyncConfig().getListenIntervals().stream()
        .map(interval -> CommonUtil.getEnumByCode(interval, IntervalEnum.class))
        .toList();
  }

  protected List<Pattern> getSubscribeSymbolPatterns() {
    return getSyncConfig().getListenSymbolPatterns().stream()
        .map(Pattern::compile)
        .toList();
  }

  protected Function<String, Boolean> getKlineEventMessageHandler() {
    return message -> {
      EventKlineEvent<?, ?> eventKlineEvent = convertToEventKlineEvent(message);
      if (eventKlineEvent == null) {
        return false;
      }
      return doForKlineEvent(eventKlineEvent, message);
    };
  }

  protected Function<String, Boolean> getTicker24HrEventMessageHandler() {
    return message -> {
      List<EventTicker24HrEvent> eventTicker24HrEvents = convertToEventTicker24HrEvent(message);
      if (CollectionUtils.isEmpty(eventTicker24HrEvents)) {
        return false;
      }
      return doForTicker24HrEvents(eventTicker24HrEvents, message);
    };
  }

  protected boolean doForKlineEvent(EventKlineEvent<?, ?> eventKlineEvent, String originalMessage) {
    if (eventKlineEvent == null || !StringUtils.equals(eventKlineEvent.getEventType(), KLINE_EVENT)) {
      WebsocketResponse websocketResponse = serializer.fromJsonString(originalMessage, WebsocketResponse.class);
      if (websocketResponse == null) {
        log.info("not handlable message received: {}", originalMessage);
      }
      return false;
    }
    Kline kline = convertToKline(eventKlineEvent);
    updateKline(eventKlineEvent.getSymbol(), eventKlineEvent.getEventKline().getInterval(), kline);
    return true;
  }

  protected boolean doForTicker24HrEvents(List<EventTicker24HrEvent> eventTicker24HrEvents, String originalMessage) {
    if (CollectionUtils.isNotEmpty(eventTicker24HrEvents)) {
      Optional<EventTicker24HrEvent> invalidEventOptional = eventTicker24HrEvents.stream()
          .filter(event -> !StringUtils.equals(event.getEventType(), TICKER_24HR_EVENT))
          .findAny();
      if (invalidEventOptional.isPresent()) {
        WebsocketResponse websocketResponse = serializer.fromJsonString(originalMessage, WebsocketResponse.class);
        if (websocketResponse == null) {
          log.info("not handlable message received: {}", originalMessage);
        }
        return false;
      }
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
  }

  protected Function<String, String> getKlineEventMessageTopicExtractor() {
    return message -> {
      EventKlineEvent<?, ?> eventKlineEvent = convertToEventKlineEvent(message);
      if (eventKlineEvent == null || !StringUtils.equals(eventKlineEvent.getEventType(), KLINE_EVENT)) {
        return null;
      }
      EventKline eventKline = eventKlineEvent.getEventKline();
      String symbol = eventKlineEvent.getSymbol();
      String interval = eventKline.getInterval();
      return buildSymbolUpdateTopic(symbol, interval);
    };
  }

  protected Function<String, String> getTicker24HrEventMessageTopicExtractor() {
    return message -> {
      List<EventTicker24HrEvent> eventTicker24HrEvents = convertToEventTicker24HrEvent(message);
      if (CollectionUtils.isNotEmpty(eventTicker24HrEvents)) {
        Optional<EventTicker24HrEvent> invalidEventOptional = eventTicker24HrEvents.stream()
            .filter(event -> !StringUtils.equals(event.getEventType(), TICKER_24HR_EVENT))
            .findAny();
        if (invalidEventOptional.isPresent()) {
          return null;
        }
        return TICKER_24HR_TOPIC;
      }
      return null;
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
    List<String> symbols = getSubscribeSymbols();
    List<IntervalEnum> subscribeIntervals = getSubscribeIntervals();
    int topicCount = symbols.size() * subscribeIntervals.size();
    registerExtraTopic(TICKER_24HR_TOPIC);
    startKlineWebSocketUpdater(topicCount);
    startKlineRpcUpdater();
    startTicker24HrRpcUpdater();
    startSymbolOfflineCleaner();
  }

  protected void registerExtraTopic(String topic) {
    this.extraSubscribeTopics.add(topic);
  }

  protected void unregisterExtraTopic(String topic) {
    this.extraSubscribeTopics.remove(topic);
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

  private List<String> getSubscribeSymbols() {
    List<Pattern> subscribeSymbolPatterns = getSubscribeSymbolPatterns();
    List<String> symbols = getSymbols();
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
    releaseSymbolKlines();
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
    return queryTicker24Hrs0();
  }

  @SuppressWarnings("unchecked")
  private List<Kline> safeQueryKlines(String symbol, String interval, Long startTime, Long endTime, Integer limit) {
    IntervalEnum intervalEnum = CommonUtil.getEnumByCode(interval, IntervalEnum.class);
    List<ImmutablePair<Long, Long>> makeUpTimeRanges = buildMakeUpTimeRanges(symbol, startTime,
        endTime, intervalEnum, limit, false);
    Long symbolOnboardTime = querySymbolOnboardTime(symbol);
    if (symbolOnboardTime >= 0) {
      makeUpTimeRanges = makeUpTimeRanges.stream()
          .filter(range -> range.getRight() >= symbolOnboardTime)
          .collect(Collectors.toList());
    }

    if (CollectionUtils.isEmpty(makeUpTimeRanges)) {
      return Collections.emptyList();
    }

    CompletableFuture<List<Kline>>[] futures = new CompletableFuture[makeUpTimeRanges.size()];
    for(int i = 0; i < makeUpTimeRanges.size(); i++) {
      ImmutablePair<Long, Long> rangePair = makeUpTimeRanges.get(i);
      CompletableFuture<List<Kline>> future = CompletableFuture.supplyAsync(() -> {
        rateLimitManager.acquire(getRateLimiterName(), getMakeUpKlinesWeight());
        List<Kline> subKlines = queryKlines0(symbol, interval,
            rangePair.getLeft(), rangePair.getRight(), getMakeUpKlinesLimit());
        for (Kline makeUpKline : subKlines) {
          updateKline(symbol, interval, makeUpKline);
        }
        return subKlines;
      }, KLINE_FETCH_EXECUTOR);
      futures[i] = future;
    }
    CompletableFuture.allOf(futures).join();
    return Arrays.stream(futures)
        .map(CompletableFuture::join)
        .flatMap(Collection::stream)
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

          Map<String, Ticker24Hr> ticker24HrMap = ticker24Hrs.stream()
              .collect(Collectors.toMap(Ticker24Hr::getSymbol, Function.identity(), (o, n) -> o));
          Set<String> needRemoveKeys = ticker24HrCache.asMap().keySet().stream()
                  .filter(existKey -> !ticker24HrMap.containsKey(existKey))
                  .collect(Collectors.toSet());
          ticker24HrCache.putAll(ticker24HrMap);
          if (CollectionUtils.isNotEmpty(needRemoveKeys)) {
            ticker24HrCache.invalidateAll(needRemoveKeys);
          }
        }), 1000, 1000 * 60, TimeUnit.MILLISECONDS
    );
  }

  private void startKlineRpcUpdater() {
    SCHEDULE_EXECUTOR_SERVICE.scheduleWithFixedDelay(
        new ExceptionSafeRunnable(() -> {
          List<IntervalEnum> subscribeIntervals = getSubscribeIntervals();
          List<String> subscribeSymbols = getSubscribeSymbols();
          List<ImmutablePair<String, IntervalEnum>> symbolIntervals = new ArrayList<>();
          for (String symbol : subscribeSymbols) {
            for (IntervalEnum interval : subscribeIntervals) {
              symbolIntervals.add(ImmutablePair.of(symbol, interval));
            }
          }
          CompletableFuture.runAsync(() -> {
            CompletableFuture<?>[] syncFutures = new CompletableFuture[symbolIntervals.size()];
            for(int i = 0; i < symbolIntervals.size(); i++) {
              ImmutablePair<String, IntervalEnum> symbolInterval = symbolIntervals.get(i);
              String symbol = symbolInterval.getLeft();
              IntervalEnum interval = symbolInterval.getRight();
              CompletableFuture<?> syncFuture = CompletableFuture.runAsync(() -> {
                int refreshKlineCount = getSyncConfig().getMinMaintainCount();
                KlineSetKey key = new KlineSetKey(symbol, interval.code());
                KlineSet klineSet = klineSetMap.get(key);
                if (klineSet != null && getMapSize(klineSet.getKlineMap(), interval) >= getSyncConfig().getRpcRefreshCount()) {
                  refreshKlineCount = getSyncConfig().getRpcRefreshCount();
                }
                safeQueryKlines(symbol, interval.code(),
                    null, System.currentTimeMillis(),
                    refreshKlineCount);
              }, KLINE_FETCH_EXECUTOR);
              syncFutures[i] = syncFuture;
            }
            CompletableFuture.allOf(syncFutures).join();
            log.info("klines for {} with intervals: {} synced.", getClass().getSimpleName(), subscribeIntervals);
          }, MANAGE_EXECUTOR).join();
        }), 1000, 1000 * 60 * 5, TimeUnit.MILLISECONDS);
  }

  private void releaseSymbolKlines() {
    SCHEDULE_EXECUTOR_SERVICE.scheduleWithFixedDelay(
        new ExceptionSafeRunnable(() -> {
          for (KlineSet klineSet : klineSetMap.values()) {
            NavigableMap<Long, Kline> klineMap = klineSet.getKlineMap();
            if (klineMap.size() > getSyncConfig().getMinMaintainCount() + 50) {
              while (klineMap.size() > getSyncConfig().getMinMaintainCount()) {
                klineMap.pollFirstEntry();
              }
            }
          }
        }), 1000, 5000, TimeUnit.MILLISECONDS);
  }

  private Set<String> buildNeedSubscribeKlineUpdateTopics(Collection<String> needSubscribeSymbols, Collection<String> needSubscribeIntervals) {
    if (CollectionUtils.isEmpty(needSubscribeSymbols) || CollectionUtils.isEmpty(needSubscribeIntervals)) {
      return Collections.emptySet();
    }
    Set<String> topics = new HashSet<>(needSubscribeSymbols.size() * needSubscribeIntervals.size());
    for (String symbol : needSubscribeSymbols) {
      for (String interval : needSubscribeIntervals) {
        String topic = buildSymbolUpdateTopic(symbol, interval);
        topics.add(topic);
      }
    }
    return topics;
  }

  private void adjustSubscribeTopics() {
    SCHEDULE_EXECUTOR_SERVICE.scheduleWithFixedDelay(
        new ExceptionSafeRunnable(() -> {
          synchronized (this) {
            Set<String> exchangeSymbols = new HashSet<>(getSubscribeSymbols());
            Set<String> needSubscribeKlineSymbols = new HashSet<>(exchangeSymbols);
            Set<String> needSubscribeKlineIntervals =
                getSubscribeIntervals().stream()
                    .map(IntervalEnum::code)
                    .collect(Collectors.toSet());
            Set<String> expectTopics = new HashSet<>(extraSubscribeTopics);
            Set<String> subscribedTopics = getSubscribedTopics();

            Set<String> needSubscribeKlineTopics =
                buildNeedSubscribeKlineUpdateTopics(needSubscribeKlineSymbols, needSubscribeKlineIntervals);
            expectTopics.addAll(needSubscribeKlineTopics);

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
        }), 1000, 5000, TimeUnit.MILLISECONDS);
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

  private Kline convertToKline(EventKlineEvent<?, ?> event) {
    if (event == null || event.getEventKline() == null) {
      return null;
    }
    EventKline eventKline = event.getEventKline();
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

  private EventKlineEvent<?, ?> convertToEventKlineEvent(String message) {
    Optional<EventKlineEvent<?, ?>> eventOptional = messageKlineEventLruCache.get(message);
    return eventOptional.orElse(null);
  }

  private List<EventTicker24HrEvent> convertToEventTicker24HrEvent(String message) {
    Optional<List<EventTicker24HrEvent>> eventOptional = messageTicker24HrEventLruCache.get(message);
    return eventOptional.orElse(null);
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
