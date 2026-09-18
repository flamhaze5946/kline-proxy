package com.zx.quant.klineproxy.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.ParsedWebSocketMessage;
import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.model.Ticker.BigDecimalTicker;
import com.zx.quant.klineproxy.model.Ticker24Hr;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties.IntervalSyncConfig;
import com.zx.quant.klineproxy.model.enums.IntervalEnum;
import com.zx.quant.klineproxy.model.exceptions.ApiException;
import com.zx.quant.klineproxy.monitor.MonitorManager;
import com.zx.quant.klineproxy.service.TickerPriceBook;
import com.zx.quant.klineproxy.util.Serializer;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

class AbstractKlineServiceTickerPriceTest {

  private static final ObjectMapper MAPPER = new ObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private static final long NOW = 1_789_716_430_000L;

  @Test
  void eachMarketSubscribesItsAllMarketStream() {
    assertThat(futures().expectedTopics()).contains("!ticker@arr").doesNotContain("!miniTicker@arr");
    assertThat(spot().expectedTopics()).contains("!miniTicker@arr").doesNotContain("!ticker@arr");

    BinanceSpotKlineServiceImpl spot = new BinanceSpotKlineServiceImpl();
    assertThat(spot.getAllMarketTickerStream()).isEqualTo(AbstractKlineService.AllMarketTickerStream.MINI_TICKER);
    assertThat(spot.getTickerStreamStaleMillis()).isEqualTo(30_000L);
    assertThat(spot.isTickerPriceSnapshotFrom24Hr()).isTrue();
    BinanceFutureKlineServiceImpl future = new BinanceFutureKlineServiceImpl();
    assertThat(future.getAllMarketTickerStream()).isEqualTo(AbstractKlineService.AllMarketTickerStream.TICKER);
    assertThat(future.getTickerStreamStaleMillis()).isEqualTo(2_000L);
    assertThat(future.isTickerPriceSnapshotFrom24Hr()).isFalse();
  }

  @Test
  void liveBookAnswersWithoutRest() {
    TickerTestService service = liveFutures();

    assertThat(prices(service.queryTickers(List.of("BTCUSDT")))).containsExactly("BTCUSDT=101");
    assertThat(service.queryTickers(List.of("BTCUSDT")).get(0).getTime()).isEqualTo(NOW - 10);
    assertThat(prices(service.queryTickers(List.of()))).containsExactly("BTCUSDT=101", "ETHUSDT=50");
    assertThat(service.symbolRestCalls.get()).isZero();
    assertThat(service.snapshotRestCalls.get()).isEqualTo(1);
  }

  @Test
  void coinMarginedContractsOnTheFuturesStreamAreIgnored() {
    TickerTestService service = liveFutures();

    service.handle(frame("24hrTicker", NOW + 10, NOW + 990, "BTCUSD_PERP", "77000"));

    assertThat(prices(service.queryTickers(List.of()))).containsExactly("BTCUSDT=101", "ETHUSDT=50");
    service.symbol24HrRest = List.of();
    assertThat(service.queryTicker24hrs(List.of("BTCUSD_PERP"))).isEmpty();
  }

  @Test
  void liveBookSendsOnlyUnknownSymbolsToRest() {
    TickerTestService service = liveFutures();
    // a listing newer than the snapshot (covering up to NOW - 1000) that did not know it
    service.symbolRest = List.of(restTicker("NEWUSDT", "7", NOW - 400));

    assertThat(prices(service.queryTickers(List.of("BTCUSDT", "NEWUSDT"))))
        .containsExactly("BTCUSDT=101", "NEWUSDT=7");
    assertThat(service.symbolRestRequests).containsExactly(List.of("NEWUSDT"));
  }

  @Test
  void allMarketReadWaitsForTheFirstSnapshotInsteadOfServingAPartialBook() {
    TickerTestService service = futures();
    service.snapshotRest = List.of(restTicker("BTCUSDT", "99", NOW - 3_000), restTicker("ETHUSDT", "50", NOW - 3_000));
    service.snapshotDelayMillis = 200L;
    service.symbolRest = List.of(restTicker("BTCUSDT", "99", NOW - 3_000));
    service.queryTickers(List.of("BTCUSDT"));  // one dated price lands before any snapshot

    service.handle(frame("24hrTicker", NOW - 1_500, NOW - 10, "BTCUSDT", "101"));  // stream alive

    assertThat(prices(service.queryTickers(List.of()))).containsExactly("BTCUSDT=101", "ETHUSDT=50");
  }

  @Test
  void symbolReadWaitsForTheFirstSnapshotWhileTheStreamIsLive() {
    TickerTestService service = futures();
    service.snapshotRest = List.of(restTicker("BTCUSDT", "99", NOW - 3_000), restTicker("ETHUSDT", "50", NOW - 3_000));
    service.snapshotDelayMillis = 200L;
    service.handle(frame("24hrTicker", NOW - 1_500, NOW - 10, "BTCUSDT", "101"));

    assertThat(prices(service.queryTickers(List.of("ETHUSDT")))).containsExactly("ETHUSDT=50");
    assertThat(service.symbolRestCalls.get()).isZero();
  }

  @Test
  void liveSpotBookLooksUpANewListingOnceWithDatedRest() {
    TickerTestService service = spot();
    service.all24HrRest = List.of(full24Hr("BTCUSDT", "99", NOW - 3_000, "98.9", 1L));
    service.handle(frame("24hrMiniTicker", NOW - 1_500, NOW - 20, "BTCUSDT", "110"));
    awaitCovered(service);
    service.symbol24HrRest = List.of(full24Hr("NEWUSDT", "7", NOW - 400, "6.9", 1L));
    service.symbolRest = List.of(restTicker("NEWUSDT", "999", 0L));

    assertThat(prices(service.queryTickers(List.of("BTCUSDT", "NEWUSDT")))).containsExactly("BTCUSDT=110", "NEWUSDT=7");
    service.handle(frame("24hrMiniTicker", NOW - 10, NOW - 5, "NEWUSDT", "8"));
    assertThat(prices(service.queryTickers(List.of("NEWUSDT")))).containsExactly("NEWUSDT=8");

    assertThat(service.symbol24HrRestCalls.get()).isEqualTo(1);
    assertThat(service.symbolRestCalls.get()).as("undated ticker/price waits for 30 silent seconds").isZero();
  }

  @Test
  void concurrentReadersShareOneRestRefreshWhileTheStreamIsSilent() throws Exception {
    TickerTestService service = futures();
    service.providerRest = List.of(restTicker("BTCUSDT", "100", NOW));
    service.snapshotDelayMillis = 200L;

    List<Thread> readers = new ArrayList<>();
    List<List<Ticker<?>>> results = java.util.Collections.synchronizedList(new ArrayList<>());
    for (int i = 0; i < 10; i++) {
      readers.add(new Thread(() -> results.add(service.queryTickers(List.of()))));
    }
    readers.forEach(Thread::start);
    for (Thread reader : readers) {
      reader.join(3_000L);
    }

    assertThat(results).hasSize(10).allSatisfy(result -> assertThat(prices(result)).containsExactly("BTCUSDT=100"));
    assertThat(service.providerRestCalls.get()).isEqualTo(1);
  }

  @Test
  void passthroughIsFetchedAgainOnceTheStreamDeliveredAnythingNewer() {
    TickerTestService service = futures();
    service.providerRest = List.of(restTicker("BTCUSDT", "100", NOW - 5_000));
    assertThat(prices(service.queryTickers(List.of()))).as("stream never seen").containsExactly("BTCUSDT=100");

    service.handle(frame("24hrTicker", NOW - 1_500, NOW - 10, "BTCUSDT", "101"));
    awaitCovered(service);
    assertThat(prices(service.queryTickers(List.of()))).containsExactly("BTCUSDT=101");

    // silent again within the passthrough's reuse window: the cached 100 predates the stream's 101
    service.providerRest = List.of(restTicker("BTCUSDT", "102", NOW + 1_500));
    service.serverTime = NOW - 10 + 2_001;
    assertThat(prices(service.queryTickers(List.of()))).containsExactly("BTCUSDT=102");
  }

  @Test
  void liveSpotStreamWithoutASnapshotNeverServesUndatedRest() {
    TickerTestService service = spot();  // ticker/24hr snapshot returns nothing
    service.symbolRest = List.of(restTicker("BTCUSDT", "99", 0L));
    service.symbol24HrRest = List.of(full24Hr("BTCUSDT", "105", NOW - 3_000, "104.9", 1L));
    service.handle(frame("24hrMiniTicker", NOW - 1_500, NOW - 20, "BTCUSDT", "110"));

    assertThatThrownBy(() -> service.queryTickers(List.of()))
        .isInstanceOfSatisfying(ApiException.class, e -> assertThat(e.getCode()).isEqualTo(-1001));
    assertThat(prices(service.queryTickers(List.of("BTCUSDT")))).as("dated lookup, then the pending stream price")
        .containsExactly("BTCUSDT=110");
    assertThat(service.symbolRestCalls.get()).isZero();
  }

  @Test
  void aSymbolWithoutAPriceAnswersEmptyEvenWithKlinesInMemory() {
    TickerTestService service = liveFutures();
    Kline.BigDecimalKline lastBar = new Kline.BigDecimalKline();
    lastBar.setOpenTime(0L);
    lastBar.setCloseTime(3_599_999L);
    lastBar.setOpenPrice(new BigDecimal("5"));
    lastBar.setHighPrice(new BigDecimal("5"));
    lastBar.setLowPrice(new BigDecimal("5"));
    lastBar.setClosePrice(new BigDecimal("5"));
    service.updateKlines("SETTLINGUSDT", IntervalEnum.ONE_HOUR.code(), List.of(lastBar));
    service.symbolRest = List.of(new BigDecimalTicker());  // Binance answers {} for settling contracts

    assertThat(service.queryTickers(List.of("SETTLINGUSDT"))).isEmpty();
  }

  @Test
  void passthroughPrefersStreamPricesThatLandedWhileItWasInFlight() {
    TickerTestService service = liveFutures();
    service.serverTime = NOW - 10 + 2_001;  // silent
    service.providerRest = List.of(restTicker("BTCUSDT", "100", NOW + 500), restTicker("ETHUSDT", "51", NOW + 500));
    service.duringProvider = () -> service.handle(frame("24hrTicker", NOW + 400, NOW + 1_990, "BTCUSDT", "110"));

    assertThat(prices(service.queryTickers(List.of()))).containsExactly("BTCUSDT=110", "ETHUSDT=51");
  }

  @Test
  void aFailingFirstSnapshotLeavesALiveSpotStreamToItsNoSnapshotPolicy() {
    TickerTestService service = spot();
    service.all24HrFailure = new ApiException(org.springframework.http.HttpStatus.BAD_GATEWAY, -1001, "timeout");
    service.symbol24HrRest = List.of(full24Hr("BTCUSDT", "105", NOW - 3_000, "104.9", 1L));
    service.handle(frame("24hrMiniTicker", NOW - 1_500, NOW - 20, "BTCUSDT", "110"));
    assertThat(await(() -> service.all24HrRestCalls.get() == 1)).as("background attempt failed").isTrue();

    pauseBeyondSnapshotRetryThrottle();  // each read below makes its own attempt, which throws
    assertThatThrownBy(() -> service.queryTickers(List.of()))
        .isInstanceOfSatisfying(ApiException.class, e -> {
          assertThat(e.getStatus()).isEqualTo(org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE);
          assertThat(e.getCode()).isEqualTo(-1001);
        });
    pauseBeyondSnapshotRetryThrottle();
    assertThat(prices(service.queryTickers(List.of("BTCUSDT")))).containsExactly("BTCUSDT=110");
    assertThat(service.all24HrRestCalls.get()).isEqualTo(3);
    assertThat(service.symbolRestCalls.get()).isZero();
  }

  @Test
  void aContractThatStoppedTradingAnswersEmptyAndLeavesTheBook() {
    TickerTestService service = liveFutures();
    service.serverTime = NOW - 10 + 2_001;  // silent
    service.symbolRest = List.of(new BigDecimalTicker());  // Binance answers {} once ETHUSDT is settling

    assertThat(service.queryTickers(List.of("ETHUSDT"))).isEmpty();
    assertThat(book(service).contains("ETHUSDT")).isFalse();
  }

  @Test
  void undatedRestIsNotServedWhenTheStreamResumesWhileItIsInFlight() {
    TickerTestService service = spot();
    service.all24HrRest = List.of(full24Hr("BTCUSDT", "99", NOW - 3_000, "98.9", 1L),
        full24Hr("ETHUSDT", "50", NOW - 60_000, "49.9", 1L));
    service.handle(frame("24hrMiniTicker", NOW - 1_500, NOW - 20, "BTCUSDT", "110"));
    awaitCovered(service);
    service.serverTime = NOW - 20 + 30_001;  // silent for spot
    Runnable resume = () -> service.handle(frame("24hrMiniTicker", NOW + 29_900, NOW + 29_990, "BTCUSDT", "111"));

    service.symbolRest = List.of(restTicker("BTCUSDT", "120", 0L));
    service.duringSymbolRest = resume;
    assertThat(prices(service.queryTickers(List.of("BTCUSDT")))).containsExactly("BTCUSDT=111");

    service.serverTime = NOW + 29_990 + 30_001;  // silent again
    service.providerRest = List.of(restTicker("BTCUSDT", "120", 0L), restTicker("ETHUSDT", "51", 0L));
    service.duringProvider = () -> service.handle(frame("24hrMiniTicker", NOW + 60_000, NOW + 60_050, "BTCUSDT", "112"));
    assertThat(prices(service.queryTickers(List.of()))).containsExactly("BTCUSDT=112", "ETHUSDT=50");

    service.serverTime = NOW + 61_100;  // let the post-gap snapshot cover the last segment
    awaitCovered(service);
  }

  @Test
  void lostFramesTriggerASnapshotAndNewerStreamPricesSurviveIt() {
    TickerTestService service = liveFutures();
    int snapshotsBefore = service.snapshotRestCalls.get();

    service.serverTime = NOW + 1_000;
    service.handle(frame("24hrTicker", NOW + 10, NOW + 990, "BTCUSDT", "102"));
    assertThat(book(service).isCovered()).as("contiguous frame").isTrue();

    // one frame lost; ETHUSDT changed only while it was missing
    service.snapshotRest = List.of(restTicker("BTCUSDT", "104", NOW + 1_500), restTicker("ETHUSDT", "55", NOW + 1_400));
    service.serverTime = NOW + 3_050;
    service.handle(frame("24hrTicker", NOW + 2_010, NOW + 2_990, "BTCUSDT", "103"));
    awaitCovered(service);

    assertThat(service.snapshotRestCalls.get()).isEqualTo(snapshotsBefore + 1);
    assertThat(prices(service.queryTickers(List.of()))).containsExactly("BTCUSDT=103", "ETHUSDT=55");
  }

  @Test
  void silentFuturesStreamFallsBackToRestAfterTwoSeconds() {
    TickerTestService service = liveFutures();
    service.symbolRest = List.of(restTicker("BTCUSDT", "100", NOW - 500));  // older than the stream event

    service.serverTime = NOW - 10 + 2_000;
    assertThat(prices(service.queryTickers(List.of("BTCUSDT")))).containsExactly("BTCUSDT=101");
    assertThat(service.symbolRestCalls.get()).isZero();

    service.serverTime = NOW - 10 + 2_001;
    assertThat(prices(service.queryTickers(List.of("BTCUSDT")))).as("dated REST merged by time")
        .containsExactly("BTCUSDT=101");
    service.symbolRest = List.of(restTicker("BTCUSDT", "102", NOW + 1_900));
    assertThat(prices(service.queryTickers(List.of("BTCUSDT")))).containsExactly("BTCUSDT=102");
    assertThat(service.symbolRestCalls.get()).isEqualTo(2);

    service.providerRest = List.of(restTicker("BTCUSDT", "103", NOW + 1_950), restTicker("XRPUSDT", "3", NOW));
    assertThat(prices(service.queryTickers(List.of()))).as("REST passthrough")
        .containsExactly("BTCUSDT=103", "XRPUSDT=3");
  }

  @Test
  void spotSnapshotsComeFromTicker24hrAndItsRestPricesAreOnlyServedAfterThirtySilentSeconds() {
    TickerTestService service = spot();
    service.all24HrRest = List.of(full24Hr("BTCUSDT", "99", NOW - 3_000, "98.9", 1L),
        full24Hr("ETHUSDT", "50", NOW - 60_000, "49.9", 1L));
    service.handle(frame("24hrMiniTicker", NOW - 1_500, NOW - 20, "BTCUSDT", "110"));
    awaitCovered(service);
    assertThat(service.all24HrRestCalls.get()).isEqualTo(1);
    assertThat(service.snapshotRestCalls.get()).as("spot ticker/price is undated").isZero();
    assertThat(prices(service.queryTickers(List.of()))).containsExactly("BTCUSDT=110", "ETHUSDT=50");

    service.symbolRest = List.of(restTicker("BTCUSDT", "120", 0L));  // spot ticker/price: no time
    service.serverTime = NOW - 20 + 30_000;
    assertThat(prices(service.queryTickers(List.of("BTCUSDT")))).containsExactly("BTCUSDT=110");
    assertThat(service.symbolRestCalls.get()).isZero();

    service.serverTime = NOW - 20 + 30_001;
    assertThat(prices(service.queryTickers(List.of("BTCUSDT")))).as("served as fetched")
        .containsExactly("BTCUSDT=120");
    assertThat(prices(book(service).get(List.of("BTCUSDT")))).as("never merged").containsExactly("BTCUSDT=110");
  }

  @Test
  void spot24hrFallbackKeepsTheNewestValueOfEachFieldGroup() {
    TickerTestService service = spot();
    service.all24HrRest = List.of(full24Hr("BTCUSDT", "99", NOW - 5_000, "99", 1L));
    service.handle(frame("24hrMiniTicker", NOW - 1_500, NOW - 1_000, "BTCUSDT", "105"));
    awaitCovered(service);

    service.handle(miniTickerFrame(NOW - 600, "BTCUSDT", "100", "110", "120", "90", "10", "1050"));
    // a full ticker newer than the last one but older than the mini ticker
    service.symbol24HrRest = List.of(full24Hr("BTCUSDT", "107", NOW - 800, "108", 2L));
    service.queryTicker24hrs(List.of("BTCUSDT"));

    service.symbol24HrRest = List.of();  // REST unavailable: answer from the cache
    Ticker24Hr cached = service.queryTicker24hrs(List.of("BTCUSDT")).get(0);
    assertThat(cached.getLastPrice()).isEqualByComparingTo("110");
    assertThat(cached.getOpenPrice()).isEqualByComparingTo("100");
    assertThat(cached.getHighPrice()).isEqualByComparingTo("120");
    assertThat(cached.getLowPrice()).isEqualByComparingTo("90");
    assertThat(cached.getPriceChange()).isEqualByComparingTo("10");
    assertThat(cached.getPriceChangePercent()).isEqualByComparingTo("10.000");
    assertThat(cached.getWeightedAvgPrice()).isEqualByComparingTo("105");
    assertThat(cached.getCloseTime()).isEqualTo(NOW - 600);
    assertThat(cached.getOpenTime()).isEqualTo(NOW - 600 - 86_400_000L);
    assertThat(cached.getBidPrice()).as("newer full-ticker field").isEqualByComparingTo("108");
    assertThat(cached.getCount()).isEqualTo(2L);

    // an older mini frame handled late changes nothing
    service.handle(miniTickerFrame(NOW - 900, "BTCUSDT", "100", "104", "120", "90", "10", "1050"));
    assertThat(service.queryTicker24hrs(List.of("BTCUSDT")).get(0).getLastPrice()).isEqualByComparingTo("110");
    assertThat(prices(book(service).get(List.of("BTCUSDT")))).containsExactly("BTCUSDT=110");
  }

  @Test
  void topicExtractorFollowsTheConfiguredStream() {
    assertThat(spot().getTicker24HrEventMessageTopicExtractor()
        .apply(miniTickerFrame(NOW, "BTCUSDT", "1", "1", "1", "1", "1", "1"))).isEqualTo("!miniTicker@arr");
    assertThat(spot().getTicker24HrEventMessageTopicExtractor()
        .apply(frame("24hrTicker", NOW, NOW, "BTCUSDT", "1"))).isNull();
    assertThat(futures().getTicker24HrEventMessageTopicExtractor()
        .apply(frame("24hrTicker", NOW, NOW, "BTCUSDT", "1"))).isEqualTo("!ticker@arr");
  }

  /** stream at NOW - 10 for BTCUSDT=101; snapshot BTCUSDT=99 / ETHUSDT=50 applied and covering */
  private static TickerTestService liveFutures() {
    TickerTestService service = futures();
    service.snapshotRest = List.of(restTicker("BTCUSDT", "99", NOW - 3_000), restTicker("ETHUSDT", "50", NOW - 60_000));
    service.handle(frame("24hrTicker", NOW - 1_500, NOW - 10, "BTCUSDT", "101"));
    // the first frame arrives before the snapshot lists BTCUSDT; it waits aside and is replayed
    awaitCovered(service);
    return service;
  }

  private static TickerTestService futures() {
    return new TickerTestService(AbstractKlineService.AllMarketTickerStream.TICKER, 2_000L, false);
  }

  private static TickerTestService spot() {
    return new TickerTestService(AbstractKlineService.AllMarketTickerStream.MINI_TICKER, 30_000L, true);
  }

  private static void awaitCovered(TickerTestService service) {
    assertThat(await(() -> book(service).isCovered())).as("book covered").isTrue();
  }

  private static void pauseBeyondSnapshotRetryThrottle() {
    try {
      Thread.sleep(550L);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static TickerPriceBook book(TickerTestService service) {
    return (TickerPriceBook) ReflectionTestUtils.getField(service, "tickerPriceBook");
  }

  private static boolean await(BooleanSupplier condition) {
    long deadline = System.currentTimeMillis() + 3_000L;
    while (System.currentTimeMillis() < deadline) {
      if (condition.getAsBoolean()) {
        return true;
      }
      try {
        Thread.sleep(10L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return condition.getAsBoolean();
  }

  private static List<String> prices(List<Ticker<?>> tickers) {
    return tickers.stream()
        .map(ticker -> ticker.getSymbol() + "=" + new BigDecimal(ticker.getPrice().toString()).toPlainString())
        .toList();
  }

  private static BigDecimalTicker restTicker(String symbol, String price, long time) {
    BigDecimalTicker ticker = new BigDecimalTicker();
    ticker.setSymbol(symbol);
    ticker.setPrice(new BigDecimal(price));
    ticker.setTime(time);
    return ticker;
  }

  private static Ticker24Hr full24Hr(String symbol, String lastPrice, long closeTime, String bidPrice, long count) {
    Ticker24Hr ticker24Hr = new Ticker24Hr();
    ticker24Hr.setSymbol(symbol);
    ticker24Hr.setLastPrice(new BigDecimal(lastPrice));
    ticker24Hr.setBidPrice(new BigDecimal(bidPrice));
    ticker24Hr.setCount(count);
    ticker24Hr.setCloseTime(closeTime);
    return ticker24Hr;
  }

  /** all-market frame spanning {@code minEventTime..eventTime}: two events for the same symbol */
  private static ParsedWebSocketMessage frame(String eventType, long minEventTime, long eventTime, String symbol,
                                              String price) {
    ArrayNode payload = MAPPER.createArrayNode();
    payload.add(event(eventType, minEventTime, symbol, price));
    payload.add(event(eventType, eventTime, symbol, price));
    return message(payload);
  }

  private static ObjectNode event(String eventType, long eventTime, String symbol, String price) {
    ObjectNode event = MAPPER.createObjectNode();
    event.put("e", eventType).put("E", eventTime).put("s", symbol).put("c", price)
        .put("o", price).put("h", price).put("l", price).put("v", "1").put("q", price);
    if (eventType.equals("24hrTicker")) {
      event.put("O", eventTime - 86_400_000L).put("C", eventTime).put("F", 1).put("L", 2).put("n", 2);
    }
    return event;
  }

  private static ParsedWebSocketMessage miniTickerFrame(long eventTime, String symbol, String open, String close,
                                                        String high, String low, String volume, String quoteVolume) {
    ArrayNode payload = MAPPER.createArrayNode();
    payload.addObject().put("e", "24hrMiniTicker").put("E", eventTime).put("s", symbol).put("c", close)
        .put("o", open).put("h", high).put("l", low).put("v", volume).put("q", quoteVolume);
    return message(payload);
  }

  private static ParsedWebSocketMessage message(JsonNode payload) {
    return new ParsedWebSocketMessage(payload.toString(), payload, payload, null, payload.get(0).path("e").asText());
  }

  private static final class TickerTestService extends AbstractKlineService<WebSocketClient> {

    private final AllMarketTickerStream stream;

    private final long staleMillis;

    private final boolean snapshotFrom24Hr;

    private final KlineSyncConfigProperties syncConfig;

    private volatile long serverTime = NOW;

    /** full-market ticker/price: the snapshot source for futures, the passthrough for spot */
    private volatile List<Ticker<?>> snapshotRest = List.of();

    /** when set, what ticker/price returns after the snapshot calls */
    private volatile List<Ticker<?>> providerRest;

    private volatile long snapshotDelayMillis;

    /** runs inside the next passthrough request, as if a frame arrived while it was in flight */
    private volatile Runnable duringProvider;

    /** runs inside the next symbol request, as if a frame arrived while it was in flight */
    private volatile Runnable duringSymbolRest;

    private volatile RuntimeException all24HrFailure;

    private volatile List<Ticker<?>> symbolRest = List.of();

    private volatile List<Ticker24Hr> all24HrRest = List.of();

    private volatile List<Ticker24Hr> symbol24HrRest = List.of();

    private final AtomicInteger snapshotRestCalls = new AtomicInteger();

    private final AtomicInteger all24HrRestCalls = new AtomicInteger();

    private final AtomicInteger providerRestCalls = new AtomicInteger();

    private final AtomicInteger symbol24HrRestCalls = new AtomicInteger();

    private final AtomicInteger symbolRestCalls = new AtomicInteger();

    private final List<List<String>> symbolRestRequests = new ArrayList<>();

    private TickerTestService(AllMarketTickerStream stream, long staleMillis, boolean snapshotFrom24Hr) {
      this.stream = stream;
      this.staleMillis = staleMillis;
      this.snapshotFrom24Hr = snapshotFrom24Hr;
      IntervalSyncConfig intervalSyncConfig = new IntervalSyncConfig();
      intervalSyncConfig.setMinMaintainCount(3);
      intervalSyncConfig.setListenSymbolPatterns(List.of(".*USDT"));
      syncConfig = new KlineSyncConfigProperties();
      syncConfig.setIntervalSyncConfigs(Map.of(IntervalEnum.ONE_HOUR.code(), intervalSyncConfig));
      this.rateLimitManager = mock(RateLimitManager.class);
      this.monitorManager = mock(MonitorManager.class);
      this.serializer = new Serializer(MAPPER);
      ReflectionTestUtils.setField(this, "numberType", "bigDecimal");
    }

    private boolean handle(ParsedWebSocketMessage message) {
      return getTicker24HrEventMessageHandler().apply(message);
    }

    private Set<String> expectedTopics() {
      return buildExpectedTopics();
    }

    @Override
    protected AllMarketTickerStream getAllMarketTickerStream() {
      return stream;
    }

    @Override
    protected long getTickerStreamStaleMillis() {
      return staleMillis;
    }

    @Override
    protected boolean isTickerPriceSnapshotFrom24Hr() {
      return snapshotFrom24Hr;
    }

    @Override
    protected List<Ticker<?>> queryTickers0() {
      Runnable during = duringProvider;
      if (providerRest != null && during != null) {
        duringProvider = null;
        during.run();
      }
      (providerRest != null ? providerRestCalls : snapshotRestCalls).incrementAndGet();
      if (snapshotDelayMillis > 0L) {
        try {
          Thread.sleep(snapshotDelayMillis);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return providerRest != null ? providerRest : snapshotRest;
    }

    @Override
    protected List<Ticker<?>> queryTickersBySymbols(Collection<String> symbols) {
      Runnable during = duringSymbolRest;
      if (during != null) {
        duringSymbolRest = null;
        during.run();
      }
      symbolRestCalls.incrementAndGet();
      synchronized (symbolRestRequests) {
        symbolRestRequests.add(List.copyOf(symbols));
      }
      return symbolRest;
    }

    @Override
    protected List<Ticker24Hr> queryTicker24Hrs0() {
      all24HrRestCalls.incrementAndGet();
      if (all24HrFailure != null) {
        throw all24HrFailure;
      }
      return all24HrRest;
    }

    @Override
    protected List<Ticker24Hr> queryTicker24HrsBySymbols(Collection<String> symbols) {
      symbol24HrRestCalls.incrementAndGet();
      return symbol24HrRest;
    }

    @Override
    protected long getServerTime() {
      return serverTime;
    }

    @Override
    protected List<Kline> queryKlines0(String symbol, String interval, Long startTime, Long endTime, Integer limit) {
      return List.of();
    }

    @Override
    protected String getRateLimiterName() {
      return "test";
    }

    @Override
    protected List<String> getSymbols() {
      return List.of("BTCUSDT");
    }

    @Override
    protected KlineSyncConfigProperties getSyncConfig() {
      return syncConfig;
    }

    @Override
    protected int getMakeUpKlinesLimit() {
      return 1;
    }

    @Override
    protected int getMakeUpKlinesWeight() {
      return 1;
    }

    @Override
    protected int getTicker24HrsWeight() {
      return 1;
    }

    @Override
    protected String getServiceType() {
      return "test";
    }

    @Override
    protected String getPersistenceServiceCode() {
      return "spot";
    }
  }
}
