package com.zx.quant.klineproxy.service.impl;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.model.CombineKlineEvent;
import com.zx.quant.klineproxy.model.EventKline;
import com.zx.quant.klineproxy.model.EventKline.BigDecimalEventKline;
import com.zx.quant.klineproxy.model.EventKline.DoubleEventKline;
import com.zx.quant.klineproxy.model.EventKlineEvent;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Kline.BigDecimalKline;
import com.zx.quant.klineproxy.model.Kline.DoubleKline;
import com.zx.quant.klineproxy.model.KlineSet;
import com.zx.quant.klineproxy.model.KlineSetKey;
import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.model.WebsocketResponse;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties;
import com.zx.quant.klineproxy.model.enums.IntervalEnum;
import com.zx.quant.klineproxy.model.enums.NumberTypeEnum;
import com.zx.quant.klineproxy.service.KlineService;
import com.zx.quant.klineproxy.util.CommonUtil;
import com.zx.quant.klineproxy.util.Serializer;
import com.zx.quant.klineproxy.util.ThreadFactoryUtil;
import java.math.BigDecimal;
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
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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

  private static final int SYMBOLS_PER_CONNECTION = 150;

  private static final String SYNC_SYMBOLS_GROUP = "symbols-sync";

  private static final ScheduledExecutorService SCHEDULE_EXECUTOR_SERVICE = new ScheduledThreadPoolExecutor(4,
      ThreadFactoryUtil.getNamedThreadFactory(SYNC_SYMBOLS_GROUP));

  protected static final Integer DEFAULT_LIMIT = 500;

  protected static final Integer MIN_LIMIT = 1;

  protected static final Integer MAX_LIMIT = 1000;

  @Autowired
  protected RateLimitManager rateLimitManager;

  @Autowired
  protected Serializer serializer;

  @Autowired
  private List<T> webSocketClients = new ArrayList<>();

  @Value("${number.type:bigDecimal}")
  private String numberType;

  private final AtomicInteger connectionCount = new AtomicInteger(0);

  private final LoadingCache<String, Optional<CombineKlineEvent>> combineMessageEventLruCache = Caffeine.newBuilder()
      .expireAfterWrite(1, TimeUnit.MINUTES)
      .maximumSize(10240)
      .build(message -> Optional.ofNullable(serializer.fromJsonString(message, CombineKlineEvent.class)));

  private final LoadingCache<String, Optional<EventKlineEvent<?, ?>>> messageEventLruCache = Caffeine.newBuilder()
      .expireAfterWrite(1, TimeUnit.MINUTES)
      .maximumSize(10240)
      .build(message -> {
        NumberTypeEnum numberTypeEnum = getNumberType();
        EventKlineEvent<?, ?> eventKlineEvent = serializer.fromJsonString(message, numberTypeEnum.eventKlineEventClass());
        return Optional.ofNullable(eventKlineEvent);
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
  protected abstract List<Kline<?>> queryKlines0(String symbol, String interval, Long startTime, Long endTime, Integer limit);

  protected abstract String getRateLimiterName();

  protected abstract List<String> getSymbols();

  protected abstract KlineSyncConfigProperties getSyncConfig();

  protected abstract int getMakeUpKlinesLimit();

  protected abstract int getMakeUpKlinesWeight();

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
      NavigableMap<Long, Kline<?>> klineMap = klineSet.getKlineMap();
      if (MapUtils.isNotEmpty(klineMap)) {
        Kline<?> kline = klineMap.lastEntry().getValue();
        Ticker<?> ticker = Ticker.create(symbol, kline, serverTime);
        tickers.add(ticker);
      }
    }
    return tickers;
  }

  @Override
  public List<Kline<?>> queryKlines(String symbol, String interval, Long startTime, Long endTime, Integer limit, boolean makeUp) {
    IntervalEnum intervalEnum = CommonUtil.getEnumByCode(interval, IntervalEnum.class);
    if (intervalEnum == null) {
      throw new RuntimeException("invalid interval");
    }
    long klinesDuration = calculateKlinesDuration(intervalEnum, limit);
    ImmutablePair<Long, Long> realTimePair = calculateRealStartEndTime(startTime, endTime, intervalEnum, limit);
    Long realStartTime = realTimePair.getLeft();
    Long realEndTime = realTimePair.getRight();

    KlineSetKey key = new KlineSetKey(symbol, intervalEnum.code());
    KlineSet klineSet = klineSetMap.computeIfAbsent(key, var -> new KlineSet(key));
    NavigableMap<Long, Kline<?>> klineSetMap = klineSet.getKlineMap();
    Map<Long, Kline<?>> savedKlineMap;

    if (makeUp) {
      List<ImmutablePair<Long, Long>> makeUpTimeRanges =
          buildMakeUpTimeRanges(symbol, startTime, endTime, intervalEnum, limit, true);

      if (CollectionUtils.isEmpty(makeUpTimeRanges)) {
        savedKlineMap = klineSetMap
            .subMap(realStartTime, true, realEndTime, true);
      } else {
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
        savedKlineMap = klineSetMap
            .subMap(realStartTime, realEndTime);
      }
    } else {
      savedKlineMap = klineSetMap
          .subMap(realStartTime, true, realEndTime, true);
    }

    if (savedKlineMap.size() < limit && startTime == null && endTime == null) {
      if (MapUtils.isEmpty(klineSetMap)) {
        return Collections.emptyList();
      }

      Kline<?> lastKline = klineSetMap.lastEntry().getValue();
      long lastKlineOpenTime = lastKline.getOpenTime();
      long startKlineOpenTime = lastKlineOpenTime - (klinesDuration * (limit - 1));
      savedKlineMap = klineSetMap
          .subMap(startKlineOpenTime, true, lastKlineOpenTime, true );
    }

    return savedKlineMap.values()
        .stream()
        .sorted(Comparator.comparing(Kline::getOpenTime))
        .toList();
  }

  @Override
  public void updateKlines(String symbol, String interval, List<Kline<?>> klines) {
    if (CollectionUtils.isEmpty(klines) || StringUtils.isBlank(symbol) || StringUtils.isBlank(interval)) {
      return;
    }
    KlineSetKey klineSetKey = new KlineSetKey(symbol, interval);
    KlineSet klineSet = klineSetMap.computeIfAbsent(klineSetKey, var -> new KlineSet(klineSetKey));
    NavigableMap<Long, Kline<?>> klineMap = klineSet.getKlineMap();
    for (Kline<?> kline : klines) {
      Kline<?> existKline = klineMap.get(kline.getOpenTime());
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

  protected Function<String, Boolean> getCombineKlineEventMessageHandler() {
    return message -> {
      CombineKlineEvent combineKlineEvent = convertToCombineKlineEvent(message);
      if (combineKlineEvent == null || StringUtils.isBlank(combineKlineEvent.getStream())) {
        return false;
      }
      EventKlineEvent<?, ?> klineEvent = combineKlineEvent.getData();
      return doForKlineEvent(klineEvent, message);
    };
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

  protected boolean doForKlineEvent(EventKlineEvent<?, ?> eventKlineEvent, String originalMessage) {
    if (eventKlineEvent == null || !StringUtils.equals(eventKlineEvent.getEventType(), KLINE_EVENT)) {
      WebsocketResponse websocketResponse = serializer.fromJsonString(originalMessage, WebsocketResponse.class);
      if (websocketResponse == null) {
        log.info("not handlable message received: {}", originalMessage);
      }
      return false;
    }
    Kline<?> kline = convertToKline(eventKlineEvent);
    updateKline(eventKlineEvent.getSymbol(), eventKlineEvent.getEventKline().getInterval(), kline);
    return true;
  }

  protected Function<String, String> getCombineKlineEventMessageTopicExtractor() {
    return message -> {
      CombineKlineEvent combineKlineEvent = convertToCombineKlineEvent(message);
      if (combineKlineEvent == null || StringUtils.isBlank(combineKlineEvent.getStream())) {
        return null;
      }
      return combineKlineEvent.getStream();
    };
  }

  protected Function<String, String> getKlineEventMessageTopicExtractor() {
    return message -> {
      EventKlineEvent<?, ?> eventKlineEvent = convertToEventKlineEvent(message);
      if (eventKlineEvent == null || !StringUtils.equals(eventKlineEvent.getEventType(), KLINE_EVENT)) {
        return null;
      }
      EventKline<?> eventKline = eventKlineEvent.getEventKline();
      String symbol = eventKlineEvent.getSymbol();
      String interval = eventKline.getInterval();
      return buildSymbolUpdateTopic(symbol, interval);
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
    startKlineWebSocketUpdater(topicCount);
    startKlineRpcUpdater();
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
      Map<Long, Kline<?>> savedKlineMap = klineSet.getKlineMap()
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
      webSocketClient.addMessageHandler(getCombineKlineEventMessageHandler());
      webSocketClient.addMessageHandler(getKlineEventMessageHandler());
      webSocketClient.addMessageTopicExtractorHandler(getCombineKlineEventMessageTopicExtractor());
      webSocketClient.addMessageTopicExtractorHandler(getKlineEventMessageTopicExtractor());
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

  protected Kline<?> serverKlineToKline(Object[] serverKline) {
    if (serverKline == null) {
      return null;
    }

    NumberTypeEnum numberTypeEnum = getNumberType();
    switch (numberTypeEnum) {
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
        kline.setIgnore((String) serverKline[11]);
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
        kline.setIgnore((String) serverKline[11]);
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

  @SuppressWarnings("unchecked")
  private List<Kline<?>> safeQueryKlines(String symbol, String interval, Long startTime, Long endTime, Integer limit) {
    IntervalEnum intervalEnum = CommonUtil.getEnumByCode(interval, IntervalEnum.class);
    List<ImmutablePair<Long, Long>> makeUpTimeRanges = buildMakeUpTimeRanges(symbol, startTime,
        endTime, intervalEnum, limit, false);
    if (CollectionUtils.isEmpty(makeUpTimeRanges)) {
      return Collections.emptyList();
    }

    CompletableFuture<List<Kline<?>>>[] futures = new CompletableFuture[makeUpTimeRanges.size()];
    for(int i = 0; i < makeUpTimeRanges.size(); i++) {
      ImmutablePair<Long, Long> rangePair = makeUpTimeRanges.get(i);
      CompletableFuture<List<Kline<?>>> future = CompletableFuture.supplyAsync(() -> {
        rateLimitManager.acquire(getRateLimiterName(), getMakeUpKlinesWeight());
        List<Kline<?>> subKlines = queryKlines0(symbol, interval,
            rangePair.getLeft(), rangePair.getRight(), getMakeUpKlinesLimit());
        for (Kline<?> makeUpKline : subKlines) {
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

  private void startKlineRpcUpdater() {
    SCHEDULE_EXECUTOR_SERVICE.scheduleWithFixedDelay(() -> {
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
          CompletableFuture<?> syncFuture = CompletableFuture.runAsync(() ->
              safeQueryKlines(symbol, interval.code(),
                  null, System.currentTimeMillis(),
                  getSyncConfig().getMinMaintainCount()), KLINE_FETCH_EXECUTOR);
          syncFutures[i] = syncFuture;
        }
        CompletableFuture.allOf(syncFutures).join();
        log.info("klines for {} with intervals: {} synced.", getClass().getSimpleName(), subscribeIntervals);
      }, MANAGE_EXECUTOR).join();
    }, 1000, 1000 * 60 * 5, TimeUnit.MILLISECONDS);
  }

  private void releaseSymbolKlines() {
    SCHEDULE_EXECUTOR_SERVICE.scheduleWithFixedDelay(
        () -> {
          for (KlineSet klineSet : klineSetMap.values()) {
            NavigableMap<Long, Kline<?>> klineMap = klineSet.getKlineMap();
            if (klineMap.size() > getSyncConfig().getMinMaintainCount() * 1.2) {
              while (klineMap.size() > getSyncConfig().getMinMaintainCount()) {
                klineMap.pollFirstEntry();
              }
            }
          }
        }, 1000, 5000, TimeUnit.MILLISECONDS);
  }

  private Set<String> buildNeedSubscribeTopics(Collection<String> needSubscribeSymbols, Collection<String> needSubscribeIntervals) {
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
        () -> {
          synchronized (this) {
            Set<String> exchangeSymbols = new HashSet<>(getSubscribeSymbols());
            Set<String> needSubscribeSymbols = new HashSet<>(exchangeSymbols);
            Set<String> needSubscribeIntervals =
                getSubscribeIntervals().stream()
                    .map(IntervalEnum::code)
                    .collect(Collectors.toSet());
            Set<String> needSubscribeTopics =
                buildNeedSubscribeTopics(needSubscribeSymbols, needSubscribeIntervals);
            Set<String> needNewSubscribeTopics = new HashSet<>(needSubscribeTopics);
            Set<String> subscribedTopics = getSubscribedTopics();
            needNewSubscribeTopics.removeAll(subscribedTopics);

            Set<String> needUnsubscribeTopics = new HashSet<>(subscribedTopics);
            needUnsubscribeTopics.removeAll(needSubscribeTopics);

            if (CollectionUtils.isNotEmpty(needNewSubscribeTopics)) {
              subscribe(needNewSubscribeTopics);
            }

            if (CollectionUtils.isNotEmpty(needUnsubscribeTopics)) {
              unsubscribe(needUnsubscribeTopics);
            }
          }
        },
        1000,
        5000,
        TimeUnit.MILLISECONDS);
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

  private Kline<?> convertToKline(EventKlineEvent<?, ?> event) {
    if (event == null || event.getEventKline() == null) {
      return null;
    }
    EventKline<?> eventKline = event.getEventKline();
    if (eventKline instanceof DoubleEventKline doubleEventKline) {
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
      doubleKline.setIgnore(doubleEventKline.getIgnore());
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
      bigDecimalKline.setIgnore(bigDecimalEventKline.getIgnore());
      return bigDecimalKline;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private CombineKlineEvent convertToCombineKlineEvent(String message) {
    Optional<CombineKlineEvent> eventOptional = combineMessageEventLruCache.get(message);
    return eventOptional.orElse(null);
  }

  private EventKlineEvent<?, ?> convertToEventKlineEvent(String message) {
    Optional<EventKlineEvent<?, ?>> eventOptional = messageEventLruCache.get(message);
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
}
