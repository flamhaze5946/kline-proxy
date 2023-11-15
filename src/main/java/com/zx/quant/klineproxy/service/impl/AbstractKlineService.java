package com.zx.quant.klineproxy.service.impl;

import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import com.zx.quant.klineproxy.model.EventKline;
import com.zx.quant.klineproxy.model.EventKlineEvent;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.KlineSet;
import com.zx.quant.klineproxy.model.KlineSetKey;
import com.zx.quant.klineproxy.model.enums.IntervalEnum;
import com.zx.quant.klineproxy.service.KlineService;
import com.zx.quant.klineproxy.util.CommonUtil;
import com.zx.quant.klineproxy.util.Serializer;
import com.zx.quant.klineproxy.util.ThreadFactoryUtil;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * abstract kline service
 * @author flamhaze5946
 */
@Slf4j
public abstract class AbstractKlineService<T extends WebSocketClient> implements KlineService, InitializingBean {

  private static final String KLINE_EVENT = "kline";

  private static final IntervalEnum WS_KLINE_INTERVAL = IntervalEnum.ONE_MINUTE;

  private static final int SYMBOLS_PER_CONNECTION = 300;

  private static final String SYNC_SYMBOLS_GROUP = "symbols-sync-";

  private static final ScheduledExecutorService EXECUTOR_SERVICE = new ScheduledThreadPoolExecutor(2,
      ThreadFactoryUtil.getNamedThreadFactory(SYNC_SYMBOLS_GROUP));

  protected static final Integer DEFAULT_LIMIT = 500;

  protected static final Integer MIN_LIMIT = 1;

  protected static final Integer MAX_LIMIT = 1500;

  @Autowired
  protected Serializer serializer;

  @Autowired
  private List<T> webSocketClients = new ArrayList<>();

  private final AtomicInteger connectionCount = new AtomicInteger(0);

  protected final Map<KlineSetKey, KlineSet> klineSetMap = new ConcurrentHashMap<>();

  protected final Set<String> subscribedSymbols = new CopyOnWriteArraySet<>();

  protected abstract List<String> getSymbols();

  protected Consumer<String> getMessageHandler() {
    return message -> {
      EventKlineEvent eventKlineEvent = serializer.fromJsonString(message, EventKlineEvent.class);
      if (eventKlineEvent == null || !StringUtils.equals(eventKlineEvent.getEventType(), KLINE_EVENT)) {
        return;
      }
      Kline kline = convertToKline(eventKlineEvent);
      updateKline(eventKlineEvent.getSymbol(), WS_KLINE_INTERVAL.code(), kline);
    };
  }

  protected String buildSymbolUpdateTopic(String symbol) {
    return StringUtils.lowerCase(symbol) + "@kline_" + WS_KLINE_INTERVAL.code();
  }

  @Override
  public void updateKline(String symbol, String interval, Kline kline) {
    KlineSetKey klineSetKey = new KlineSetKey(symbol, interval);
    KlineSet klineSet = klineSetMap.computeIfAbsent(klineSetKey, var -> new KlineSet(klineSetKey));
    NavigableMap<Long, Kline> klineMap = klineSet.getKlineMap();
    Kline existKline = klineMap.get(kline.getOpenTime());
    if (existKline == null || existKline.getTradeNum() < kline.getTradeNum()) {
      klineSet.getKlineMap().put(kline.getOpenTime(), kline);
    }

    IntervalEnum intervalEnum = CommonUtil.getEnumByCode(interval, IntervalEnum.class);
    if (intervalEnum == null) {
      throw new RuntimeException("invalid interval");
    }
    log.info("kline: {}", kline);
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    start();
  }

  protected void start() {
    List<String> symbols = getSymbols();
    connectionCount.set(Math.min(webSocketClients.size(), symbols.size() / SYMBOLS_PER_CONNECTION + 1));
    for(int i = 0; i < connectionCount.get(); i++) {
      T webSocketClient = webSocketClients.get(i);
      webSocketClient.addMessageHandler(getMessageHandler());
      webSocketClient.start();
    }
    EXECUTOR_SERVICE.scheduleWithFixedDelay(
        () -> {
          synchronized (subscribedSymbols) {
            Set<String> exchangeSymbols = new HashSet<>(getSymbols());
            Set<String> needSubscribeSymbols = new HashSet<>(exchangeSymbols);
            needSubscribeSymbols.removeAll(subscribedSymbols);

            Set<String> needUnsubscribeSymbols = new HashSet<>(subscribedSymbols);
            needUnsubscribeSymbols.removeAll(exchangeSymbols);

            if (CollectionUtils.isNotEmpty(needSubscribeSymbols)) {
              Set<String> topics = needSubscribeSymbols.stream()
                      .map(this::buildSymbolUpdateTopic)
                      .collect(Collectors.toSet());
              subscribe(topics);
            }

            if (CollectionUtils.isNotEmpty(needUnsubscribeSymbols)) {
              Set<String> topics = needUnsubscribeSymbols.stream()
                  .map(this::buildSymbolUpdateTopic)
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
}
