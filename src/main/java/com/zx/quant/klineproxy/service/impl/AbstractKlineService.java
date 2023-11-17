package com.zx.quant.klineproxy.service.impl;

import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.model.EventKline;
import com.zx.quant.klineproxy.model.EventKlineEvent;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.KlineSet;
import com.zx.quant.klineproxy.model.KlineSetKey;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties;
import com.zx.quant.klineproxy.model.enums.IntervalEnum;
import com.zx.quant.klineproxy.service.KlineService;
import com.zx.quant.klineproxy.util.CommonUtil;
import com.zx.quant.klineproxy.util.Serializer;
import com.zx.quant.klineproxy.util.ThreadFactoryUtil;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
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
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

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

  private static final String KLINE_EVENT = "kline";

  private static final int SYMBOLS_PER_CONNECTION = 800;

  private static final String SYNC_SYMBOLS_GROUP = "symbols-sync-";

  private static final ScheduledExecutorService SCHEDULE_EXECUTOR_SERVICE = new ScheduledThreadPoolExecutor(4,
      ThreadFactoryUtil.getNamedThreadFactory(SYNC_SYMBOLS_GROUP));

  private static final Integer MAKE_UP_LIMIT = 499;

  protected static final Integer DEFAULT_LIMIT = 500;

  protected static final Integer MIN_LIMIT = 1;

  protected static final Integer MAX_LIMIT = 1000;

  @Autowired
  protected RateLimitManager rateLimitManager;

  @Autowired
  protected Serializer serializer;

  @Autowired
  private List<T> webSocketClients = new ArrayList<>();

  private final AtomicInteger connectionCount = new AtomicInteger(0);

  protected final Map<KlineSetKey, KlineSet> klineSetMap = new ConcurrentHashMap<>();

  protected final Set<String> subscribedSymbols = new CopyOnWriteArraySet<>();

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

  protected abstract String getRateLimiterName();

  protected abstract List<String> getSymbols();

  protected abstract KlineSyncConfigProperties getSyncConfig();

  @Override
  public List<Kline> queryKlines(String symbol, String interval, Long startTime, Long endTime, Integer limit) {
    IntervalEnum intervalEnum = CommonUtil.getEnumByCode(interval, IntervalEnum.class);
    if (intervalEnum == null) {
      throw new RuntimeException("invalid interval");
    }
    int realLimit = limit != null ? limit : DEFAULT_LIMIT;
    realLimit = Math.min(realLimit, MAX_LIMIT);
    realLimit = Math.max(realLimit, MIN_LIMIT);
    long intervalMills = intervalEnum.getMills();
    long klinesDuration = (intervalMills * (realLimit - 1));
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
    KlineSetKey key = new KlineSetKey(symbol, interval);
    KlineSet klineSet = klineSetMap.computeIfAbsent(key, var -> new KlineSet(key));
    SortedMap<Long, Kline> savedKlineMap = klineSet.getKlineMap()
        .subMap(realStartTime, true, realEndTime, true);
    NavigableSet<Long> needMakeUpOpenTimes = new TreeSet<>();
    long indexTime = realStartTime;
    while (indexTime <= realEndTime) {
      needMakeUpOpenTimes.add(indexTime);
      indexTime += intervalMills;
    }
    needMakeUpOpenTimes.removeAll(savedKlineMap.keySet());
    if (CollectionUtils.isNotEmpty(needMakeUpOpenTimes)) {
      List<ImmutablePair<Long, Long>> makeUpTimeRanges = new ArrayList<>();
      if (needMakeUpOpenTimes.size() > 1) {
        Long makeStartTime = null;
        Long makeEndTime = null;
        for (Long needMakeUpOpenTime : needMakeUpOpenTimes) {
          if (makeStartTime == null) {
            makeStartTime = needMakeUpOpenTime;
            makeEndTime = makeStartTime + ((MAKE_UP_LIMIT - 1) * intervalMills);
            makeUpTimeRanges.add(ImmutablePair.of(makeStartTime, makeEndTime));
            continue;
          }
          if (needMakeUpOpenTime > makeEndTime) {
            makeStartTime = needMakeUpOpenTime;
            makeEndTime = makeStartTime + ((MAKE_UP_LIMIT - 1) * intervalMills);
            makeUpTimeRanges.add(ImmutablePair.of(makeStartTime, makeEndTime));
          }
        }
      } else {
        Long makeUpKlineTime = needMakeUpOpenTimes.first();
        makeUpTimeRanges.add(ImmutablePair.of(makeUpKlineTime, makeUpKlineTime + 1));
      }

      CompletableFuture<?>[] futures = new CompletableFuture[makeUpTimeRanges.size()];
      for(int i = 0; i < makeUpTimeRanges.size(); i++) {
        ImmutablePair<Long, Long> makeUpRange = makeUpTimeRanges.get(i);
        CompletableFuture<?> makeUpFuture = CompletableFuture.runAsync(() ->
            safeQueryKlines(symbol, interval,
                makeUpRange.getLeft(), makeUpRange.getRight(), MAKE_UP_LIMIT)
            , KLINE_FETCH_EXECUTOR);
        futures[i] = makeUpFuture;
      }
      CompletableFuture.allOf(futures).join();
      savedKlineMap = klineSet.getKlineMap()
          .subMap(realStartTime, realEndTime);
    }

    if (savedKlineMap.size() < realLimit && startTime == null && endTime == null) {
      Kline lastKline = klineSet.getKlineMap().lastEntry().getValue();
      long lastKlineOpenTime = lastKline.getOpenTime();
      long startKlineOpenTime = lastKlineOpenTime - (klinesDuration * (realLimit - 1));
      savedKlineMap = klineSet.getKlineMap()
          .subMap(startKlineOpenTime, true, lastKlineOpenTime, true );
    }

    return savedKlineMap.values()
        .stream()
        .sorted(Comparator.comparing(Kline::getOpenTime))
        .toList();
  }

  @Override
  public void updateKline(String symbol, String interval, Kline kline) {
    if (kline == null) {
      return;
    }
    KlineSetKey klineSetKey = new KlineSetKey(symbol, interval);
    KlineSet klineSet = klineSetMap.computeIfAbsent(klineSetKey, var -> new KlineSet(klineSetKey));
    NavigableMap<Long, Kline> klineMap = klineSet.getKlineMap();
    Kline existKline = klineMap.get(kline.getOpenTime());
    if (existKline == null || existKline.getTradeNum() < kline.getTradeNum()) {
      klineSet.getKlineMap().put(kline.getOpenTime(), kline);
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

  protected Consumer<String> getMessageHandler() {
    return message -> {
      EventKlineEvent eventKlineEvent = serializer.fromJsonString(message, EventKlineEvent.class);
      if (eventKlineEvent == null || !StringUtils.equals(eventKlineEvent.getEventType(), KLINE_EVENT)) {
        return;
      }
      Kline kline = convertToKline(eventKlineEvent);
      updateKline(eventKlineEvent.getSymbol(), eventKlineEvent.getEventKline().getInterval(), kline);
    };
  }

  protected String buildSymbolUpdateTopic(String symbol, IntervalEnum intervalEnum) {
    return StringUtils.lowerCase(symbol) + "@kline_" + intervalEnum.code();
  }

  protected List<String> buildSymbolUpdateTopics(String symbol) {
    List<IntervalEnum> intervalEnums = getSyncConfig().getListenIntervals().stream()
        .map(interval -> CommonUtil.getEnumByCode(interval, IntervalEnum.class))
        .toList();
    return intervalEnums.stream()
        .map(intervalEnum -> buildSymbolUpdateTopic(symbol, intervalEnum))
        .collect(Collectors.toList());
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
      webSocketClient.addMessageHandler(getMessageHandler());
      webSocketClient.start();
    }
    adjustSubscribeSymbols();
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

  protected Kline serverKlineToKline(Object[] serverKline) {
    if (serverKline == null) {
      return null;
    }

    Kline kline = new Kline();
    kline.setOpenTime((Long) serverKline[0]);
    kline.setOpenPrice(new BigDecimal((String) serverKline[1]));
    kline.setHighPrice(new BigDecimal((String) serverKline[2]));
    kline.setLowPrice(new BigDecimal((String) serverKline[3]));
    kline.setClosePrice(new BigDecimal((String) serverKline[4]));
    kline.setVolume(new BigDecimal((String) serverKline[5]));
    kline.setCloseTime((Long) serverKline[6]);
    kline.setQuoteVolume(new BigDecimal((String) serverKline[7]));
    kline.setTradeNum((Integer) serverKline[8]);
    kline.setActiveBuyVolume(new BigDecimal((String) serverKline[9]));
    kline.setActiveBuyQuoteVolume(new BigDecimal((String) serverKline[10]));
    kline.setIgnore((String) serverKline[11]);
    return kline;
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

  private List<Kline> safeQueryKlines(String symbol, String interval, Long startTime, Long endTime, Integer limit) {
    rateLimitManager.acquire(getRateLimiterName(), 1);
    List<Kline> klines = queryKlines0(symbol, interval,
        startTime, endTime, limit);
    for (Kline makeUpKline : klines) {
      updateKline(symbol, interval, makeUpKline);
    }
    return klines;
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
          CompletableFuture<?> syncFuture = CompletableFuture.runAsync(() -> {
            safeQueryKlines(symbol, interval.code(), null, System.currentTimeMillis(), getSyncConfig().getMinMaintainCount());
          }, KLINE_FETCH_EXECUTOR);
          syncFutures[i] = syncFuture;
        }
        CompletableFuture.allOf(syncFutures).join();
        log.info("klines for {} with intervals: {} synced.", getClass().getSimpleName(), subscribeIntervals);
      }, MANAGE_EXECUTOR).join();
    }, 1000, 1000 * 60 * 5, TimeUnit.MILLISECONDS);
  }

  private void adjustSubscribeSymbols() {
    SCHEDULE_EXECUTOR_SERVICE.scheduleWithFixedDelay(
        () -> {
          synchronized (subscribedSymbols) {
            Set<String> exchangeSymbols = new HashSet<>(getSubscribeSymbols());
            Set<String> needSubscribeSymbols = new HashSet<>(exchangeSymbols);
            needSubscribeSymbols.removeAll(subscribedSymbols);

            Set<String> needUnsubscribeSymbols = new HashSet<>(subscribedSymbols);
            needUnsubscribeSymbols.removeAll(exchangeSymbols);

            if (CollectionUtils.isNotEmpty(needSubscribeSymbols)) {
              Set<String> topics = needSubscribeSymbols.stream()
                  .map(this::buildSymbolUpdateTopics)
                  .flatMap(Collection::stream)
                  .collect(Collectors.toSet());
              subscribe(topics);
            }

            if (CollectionUtils.isNotEmpty(needUnsubscribeSymbols)) {
              Set<String> topics = needUnsubscribeSymbols.stream()
                  .map(this::buildSymbolUpdateTopics)
                  .flatMap(Collection::stream)
                  .collect(Collectors.toSet());
              unsubscribe(topics);
            }

            subscribedSymbols.addAll(exchangeSymbols);
            subscribedSymbols.removeAll(needUnsubscribeSymbols);
          }
        },
        1000,
        5000,
        TimeUnit.MILLISECONDS);
  }

  private List<T> getActiveWebSocketClients() {
    return webSocketClients.subList(0, connectionCount.get());
  }

  private Kline convertToKline(EventKlineEvent event) {
    if (event == null || event.getEventKline() == null) {
      return null;
    }
    EventKline eventKline = event.getEventKline();
    Kline kline = new Kline();
    kline.setOpenTime(eventKline.getOpenTime());
    kline.setCloseTime(eventKline.getCloseTime());
    kline.setOpenPrice(eventKline.getOpenPrice());
    kline.setHighPrice(eventKline.getHighPrice());
    kline.setLowPrice(eventKline.getLowPrice());
    kline.setClosePrice(eventKline.getClosePrice());
    kline.setVolume(eventKline.getVolume());
    kline.setQuoteVolume(eventKline.getQuoteVolume());
    kline.setTradeNum(eventKline.getTradeNum());
    kline.setActiveBuyVolume(eventKline.getActiveBuyVolume());
    kline.setActiveBuyQuoteVolume(eventKline.getActiveBuyQuoteVolume());
    kline.setIgnore(eventKline.getIgnore());

    return kline;
  }

  private static ExecutorService buildKlineFetchExecutor() {
    ThreadFactory namedThreadFactory = ThreadFactoryUtil.getNamedThreadFactory(
        KLINE_FETCH_EXECUTOR_GROUP);
    return new ThreadPoolExecutor(0, 20,
        1, TimeUnit.MINUTES, new SynchronousQueue<>(),
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
}
