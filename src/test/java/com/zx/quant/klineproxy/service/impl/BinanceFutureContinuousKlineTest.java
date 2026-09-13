package com.zx.quant.klineproxy.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zx.quant.klineproxy.client.model.BinanceFutureExchange;
import com.zx.quant.klineproxy.client.model.BinanceFutureSymbol;
import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.model.BulkKlinesResponse;
import com.zx.quant.klineproxy.model.KlineSetKey;
import com.zx.quant.klineproxy.model.ParsedWebSocketMessage;
import com.zx.quant.klineproxy.model.config.KlineBulkProperties;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties.BinanceFutureKlineSyncConfigProperties;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties.IntervalSyncFutureConfig;
import com.zx.quant.klineproxy.monitor.MonitorManager;
import com.zx.quant.klineproxy.service.ExchangeService;
import com.zx.quant.klineproxy.util.Serializer;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.test.util.ReflectionTestUtils;

class BinanceFutureContinuousKlineTest {

  private static final long BOUNDARY = 1_789_290_000_000L;
  private static final String ORDINARY_TOPIC = "adausdt@kline_1h";
  private static final String CONTINUOUS_TOPIC = "adausdt_perpetual@continuousKline_1h";
  private static final ObjectMapper MAPPER = new ObjectMapper()
      .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

  @Test
  void configurationBindsPerIntervalAndDefaultsToOrdinary() {
    assertThat(new IntervalSyncFutureConfig().isUseContinuousKlineStream()).isFalse();
    var source = new MapConfigurationPropertySource(Map.of(
        "kline.binance.future.interval-sync-configs[1h].use-continuous-kline-stream", "true",
        "kline.binance.future.interval-sync-configs[1d].min-maintain-count", "1000"));
    var config = new Binder(source).bind("kline.binance.future",
        Bindable.of(BinanceFutureKlineSyncConfigProperties.class)).get();

    assertThat(config.getIntervalSyncConfigs().get("1h").isUseContinuousKlineStream()).isTrue();
    assertThat(config.getIntervalSyncConfigs().get("1d").isUseContinuousKlineStream()).isFalse();
  }

  @Test
  void selectsTopicsPerIntervalAndInvalidatesCachedTopicsWhenToggled() {
    Fixture f = fixture(false, "string", List.of(symbol("ADAUSDT", "ADAUSDT", "PERPETUAL")));
    Set<String> ordinary = f.service.buildExpectedTopics();
    assertThat(ordinary).containsExactlyInAnyOrder("!ticker@arr", ORDINARY_TOPIC, "adausdt@kline_1d");
    assertThat(f.service.buildExpectedTopics()).isSameAs(ordinary);

    f.config.getIntervalSyncConfigs().get("1h").setUseContinuousKlineStream(true);
    Set<String> continuous = f.service.buildExpectedTopics();
    assertThat(continuous).containsExactlyInAnyOrder("!ticker@arr", CONTINUOUS_TOPIC, "adausdt@kline_1d");
    assertThat(f.service.buildExpectedTopics()).isSameAs(continuous);

    f.config.getIntervalSyncConfigs().get("1h").setUseContinuousKlineStream(false);
    assertThat(f.service.buildExpectedTopics()).isEqualTo(ordinary);
  }

  @Test
  void metadataRefreshChangesTopicsEvenWhenSymbolsAndIntervalsDoNotChange() {
    Fixture f = fixture(true, "string", List.of(symbol("ADAUSDT", "ADAUSDT", "PERPETUAL")));
    Set<String> before = f.service.buildExpectedTopics();
    replaceExchange(f, List.of(symbol("ADAUSDT", "ADAUSDT", "TRADIFI_PERPETUAL")));

    assertThat(f.service.buildExpectedTopics()).contains("adausdt_tradifi_perpetual@continuousKline_1h")
        .doesNotContain(CONTINUOUS_TOPIC).isNotEqualTo(before);
  }

  @Test
  void rollingDatedAndUnknownContractsKeepOrdinaryStreams() {
    Fixture f = fixture(true, "string", List.of(
        symbol("ADAUSDT", "ADAUSDT", "PERPETUAL"),
        symbol("BTCUSDT_261225", "BTCUSDT", "CURRENT_QUARTER"),
        symbol("UNKNOWNUSDT", "UNKNOWNUSDT", "UNKNOWN"),
        symbol("MISSINGUSDT", null, "PERPETUAL")));

    assertThat(f.service.buildExpectedTopics()).contains(CONTINUOUS_TOPIC,
        "btcusdt_261225@kline_1h", "unknownusdt@kline_1h", "missingusdt@kline_1h")
        .doesNotContain("btcusdt_current_quarter@continuousKline_1h");
  }

  @Test
  void ambiguousPairMappingsFallBackAndNeverAssignOneContractsBarToAnother() throws Exception {
    Fixture f = fixture(true, "string", List.of(
        symbol("ADAUSDT", "ADAUSDT", "PERPETUAL"),
        symbol("ADAUSDT_OTHER", "ADAUSDT", "PERPETUAL")));
    assertThat(f.service.buildExpectedTopics()).contains(ORDINARY_TOPIC, "adausdt_other@kline_1h")
        .doesNotContain(CONTINUOUS_TOPIC);
    assertThat(f.service.getKlineEventMessageHandler().apply(message(fixtureMessage(true), false))).isFalse();
    assertThat(f.service.klineSetMap).isEmpty();
  }

  @Test
  void mapsPairToActualSymbolAndRejectsStaleMappingAfterExchangeRefresh() throws Exception {
    Fixture f = fixture(true, "string", List.of(symbol("ADAUSDT_ACTUAL", "ADAUSDT", "PERPETUAL")));
    assertThat(f.service.getKlineEventMessageHandler().apply(message(fixtureMessage(true), false))).isTrue();
    assertThat(f.service.klineSetMap).containsKey(new KlineSetKey("ADAUSDT_ACTUAL", "1h"))
        .doesNotContainKey(new KlineSetKey("ADAUSDT", "1h"));

    BinanceFutureSymbol halted = symbol("ADAUSDT_ACTUAL", "ADAUSDT", "PERPETUAL");
    halted.setStatus("SETTLING");
    replaceExchange(f, List.of(halted));
    assertThat(f.service.getKlineEventMessageHandler().apply(message(fixtureMessage(true), false))).isFalse();
  }

  @Test
  void heartbeatUsesIncomingTopicRatherThanCurrentConfiguredMode() throws Exception {
    Fixture f = fixture(true, "string", List.of(symbol("ADAUSDT", "ADAUSDT", "PERPETUAL")));
    var extractor = f.service.getKlineEventMessageTopicExtractor();
    assertThat(extractor.apply(message(fixtureMessage(false), false))).isEqualTo(ORDINARY_TOPIC);
    assertThat(extractor.apply(message(fixtureMessage(true), false))).isEqualTo(CONTINUOUS_TOPIC);
    assertThat(extractor.apply(message(fixtureMessage(true), true))).isEqualTo(CONTINUOUS_TOPIC);
    f.config.getIntervalSyncConfigs().get("1h").setUseContinuousKlineStream(false);
    assertThat(extractor.apply(message(fixtureMessage(true), false))).isEqualTo(CONTINUOUS_TOPIC);
  }

  static Stream<Arguments> messageFormats() {
    return Stream.of("string", "float", "double", "bigDecimal")
        .flatMap(type -> Stream.of(Arguments.of(type, false), Arguments.of(type, true)));
  }

  /** Paired live messages: ADAUSDT, 2026-09-13 09:00 UTC, captured before this implementation. */
  @ParameterizedTest
  @MethodSource("messageFormats")
  void bothStreamsPreserveBarFieldsAndOnlyXTrueFinalizes(String numberType, boolean combined) throws Exception {
    Fixture ordinary = fixture(false, numberType, List.of(symbol("ADAUSDT", "ADAUSDT", "PERPETUAL")));
    Fixture continuous = fixture(true, numberType, List.of(symbol("ADAUSDT", "ADAUSDT", "PERPETUAL")));
    for (boolean useContinuous : List.of(false, true)) {
      Fixture f = useContinuous ? continuous : ordinary;
      ObjectNode finalMessage = fixtureMessage(useContinuous);
      ObjectNode forming = finalMessage.deepCopy();
      ((ObjectNode) forming.get("k")).put("x", false);
      // Same trade count: receiving x=true alone must still mark the bar final.
      assertThat(f.service.getKlineEventMessageHandler().apply(message(forming, combined))).isTrue();
      BulkKlinesResponse pending = f.service.queryBulkKlines("1h", 1, true, List.of("ADAUSDT"));
      assertThat(pending.finalized()).isFalse();
      assertThat(pending.pending()).containsExactly("ADAUSDT");

      assertThat(f.service.getKlineEventMessageHandler().apply(message(finalMessage, combined))).isTrue();
      BulkKlinesResponse done = f.service.queryBulkKlines("1h", 1, true, List.of("ADAUSDT"));
      assertThat(done.finalized()).isTrue();
      assertThat(done.pending()).isEmpty();
      assertThat(done.klines()).containsOnlyKeys("ADAUSDT");
      assertThat(done.klines().get("ADAUSDT")).hasSize(1);
      Object[] row = done.klines().get("ADAUSDT").getFirst();
      assertThat(row[0]).isEqualTo(BOUNDARY - 3_600_000L);
      assertThat(row[6]).isEqualTo(BOUNDARY - 1);
      assertThat(row[8]).isEqualTo(finalMessage.path("k").path("n").asInt());
      assertThat(f.service.getLastClosedBarSettle("1h").arrived()).isEqualTo(1);
      if (numberType.equals("string") || numberType.equals("bigDecimal")) {
        String[] numericFields = {"o", "h", "l", "c", "v", "q", "V", "Q"};
        int[] indices = {1, 2, 3, 4, 5, 7, 9, 10};
        for (int i = 0; i < indices.length; i++) {
          assertThat(new BigDecimal(row[indices[i]].toString()))
              .isEqualByComparingTo(new BigDecimal(finalMessage.path("k").path(numericFields[i]).asText()));
        }
      }
      ObjectNode stale = finalMessage.deepCopy();
      ((ObjectNode) stale.get("k")).put("x", false).put("c", "0.01")
          .put("n", finalMessage.path("k").path("n").asInt() - 1);
      assertThat(f.service.getKlineEventMessageHandler().apply(message(stale, combined))).isTrue();
      assertArrayEquals(row, com.zx.quant.klineproxy.util.ConvertUtil.convertToDisplayKline(
          f.service.klineSetMap.get(new KlineSetKey("ADAUSDT", "1h")).getKlineMap().lastEntry().getValue()));
    }
    assertArrayEquals(ordinary.service.queryBulkKlines("1h", 1, true, List.of("ADAUSDT")).klines().get("ADAUSDT").getFirst(),
        continuous.service.queryBulkKlines("1h", 1, true, List.of("ADAUSDT")).klines().get("ADAUSDT").getFirst());
  }

  @Test
  void ordinaryMessageStillWorksWhenContinuousModeIsEnabled() throws Exception {
    Fixture f = fixture(true, "string", List.of(symbol("ADAUSDT", "ADAUSDT", "PERPETUAL")));
    assertThat(f.service.getKlineEventMessageHandler().apply(message(fixtureMessage(false), false))).isTrue();
    assertThat(f.service.queryBulkKlines("1h", 1, true, List.of("ADAUSDT")).finalized()).isTrue();
  }

  @Test
  void spotDoesNotAcceptContinuousMessages() throws Exception {
    BinanceSpotKlineServiceImpl spot = new BinanceSpotKlineServiceImpl();
    assertThat(spot.getKlineEventMessageHandler().apply(message(fixtureMessage(true), false))).isFalse();
    assertThat(spot.getKlineEventMessageTopicExtractor().apply(message(fixtureMessage(true), false))).isNull();
  }

  @Test
  void invalidContinuousRoutingAndMissingBarAreIgnored() throws Exception {
    Fixture f = fixture(true, "string", List.of(symbol("ADAUSDT", "ADAUSDT", "PERPETUAL")));
    for (String field : List.of("ps", "ct", "k")) {
      ObjectNode invalid = fixtureMessage(true);
      invalid.remove(field);
      assertThat(f.service.getKlineEventMessageHandler().apply(message(invalid, false))).isFalse();
      assertThat(f.service.getKlineEventMessageTopicExtractor().apply(message(invalid, false))).isNull();
    }
    ObjectNode quarterly = fixtureMessage(true).put("ct", "CURRENT_QUARTER");
    assertThat(f.service.getKlineEventMessageHandler().apply(message(quarterly, false))).isFalse();
    ObjectNode unsupportedInterval = fixtureMessage(true);
    ((ObjectNode) unsupportedInterval.get("k")).put("i", "invalid");
    assertThat(f.service.getKlineEventMessageHandler().apply(message(unsupportedInterval, false))).isFalse();
    assertThat(f.service.klineSetMap).isEmpty();
  }

  private static ObjectNode fixtureMessage(boolean continuous) throws Exception {
    String path = "/binance/" + (continuous ? "continuous_kline" : "kline") + "-1h.json";
    try (var input = BinanceFutureContinuousKlineTest.class.getResourceAsStream(path)) {
      assertThat(input).isNotNull();
      return (ObjectNode) MAPPER.readTree(input);
    }
  }

  private static ParsedWebSocketMessage message(ObjectNode payload, boolean combined) {
    String topic = payload.path("e").asText().equals("continuous_kline") ? CONTINUOUS_TOPIC : ORDINARY_TOPIC;
    JsonNode root = combined ? MAPPER.createObjectNode().put("stream", topic).set("data", payload) : payload;
    return new ParsedWebSocketMessage(root.toString(), root, payload, combined ? topic : null, payload.path("e").asText());
  }

  private static Fixture fixture(boolean continuous, String numberType, List<BinanceFutureSymbol> symbols) {
    BinanceFutureKlineServiceImpl service = new BinanceFutureKlineServiceImpl();
    BinanceFutureKlineSyncConfigProperties config = new BinanceFutureKlineSyncConfigProperties();
    IntervalSyncFutureConfig hour = new IntervalSyncFutureConfig();
    hour.setListenSymbolPatterns(List.of(".*"));
    hour.setUseContinuousKlineStream(continuous);
    IntervalSyncFutureConfig day = new IntervalSyncFutureConfig();
    day.setListenSymbolPatterns(List.of(".*"));
    config.setIntervalSyncConfigs(Map.of("1h", hour, "1d", day));
    @SuppressWarnings("unchecked")
    ExchangeService<BinanceFutureExchange> exchange = mock(ExchangeService.class);
    when(exchange.queryServerTime()).thenReturn(BOUNDARY + 150L);
    ReflectionTestUtils.setField(service, "klineSyncConfigProperties", config);
    ReflectionTestUtils.setField(service, "exchangeService", exchange);
    ReflectionTestUtils.setField(service, "serializer", new Serializer(MAPPER));
    ReflectionTestUtils.setField(service, "monitorManager", mock(MonitorManager.class));
    ReflectionTestUtils.setField(service, "rateLimitManager", mock(RateLimitManager.class));
    ReflectionTestUtils.setField(service, "numberType", numberType);
    KlineBulkProperties bulk = new KlineBulkProperties();
    bulk.setFinalWaitMaxMs(0L);
    ReflectionTestUtils.setField(service, "bulkProperties", bulk);
    Fixture f = new Fixture(service, config, exchange);
    replaceExchange(f, symbols);
    return f;
  }

  private static void replaceExchange(Fixture f, List<BinanceFutureSymbol> symbols) {
    BinanceFutureExchange exchange = new BinanceFutureExchange();
    exchange.setSymbols(symbols);
    when(f.exchange.queryExchange()).thenReturn(exchange);
    when(f.exchange.querySymbols()).thenReturn(symbols.stream().filter(s -> s.getStatus().equals("TRADING"))
        .map(BinanceFutureSymbol::getSymbol).toList());
  }

  private static BinanceFutureSymbol symbol(String symbol, String pair, String contractType) {
    BinanceFutureSymbol result = new BinanceFutureSymbol();
    result.setSymbol(symbol);
    result.setPair(pair);
    result.setContractType(contractType);
    result.setStatus("TRADING");
    return result;
  }

  private record Fixture(BinanceFutureKlineServiceImpl service, BinanceFutureKlineSyncConfigProperties config,
                         ExchangeService<BinanceFutureExchange> exchange) {
  }
}
