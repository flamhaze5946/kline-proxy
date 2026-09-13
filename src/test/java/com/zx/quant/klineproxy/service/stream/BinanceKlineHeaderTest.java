package com.zx.quant.klineproxy.service.stream;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zx.quant.klineproxy.client.model.BinanceFutureExchange;
import com.zx.quant.klineproxy.client.model.BinanceFutureSymbol;
import com.zx.quant.klineproxy.util.Serializer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class BinanceKlineHeaderTest {
  private final Serializer serializer = new Serializer(new ObjectMapper());
  private static final String KLINE = "{\"e\":\"kline\",\"s\":\"BTCUSDT\",\"E\":100,\"k\":{\"t\":0,\"i\":\"1h\",\"x\":true,\"n\":20}}";

  @Test
  void rawAndCombinedIncludingDataBeforeStreamHaveTheSamePayloadIdentity() {
    var raw = BinanceKlineHeader.parse(KLINE, serializer);
    var combined = BinanceKlineHeader.parse("{\"data\":" + KLINE + ",\"stream\":\"btcusdt@kline_1h\"}", serializer);
    assertThat(raw.closed()).isTrue();
    assertThat(raw.openTime()).isZero();
    assertThat(combined.symbol()).isEqualTo(raw.symbol());
    assertThat(combined.tradeCount()).isEqualTo(raw.tradeCount());
    assertThat(combined.eventTime()).isEqualTo(100L);
    assertThat(combined.stream()).isEqualTo("btcusdt@kline_1h");
  }

  @Test
  void malformedAmbiguousAndControlFramesFallBackInsteadOfBeingCoalesced() {
    for (String raw : List.of("PING", "{", "{\"result\":null,\"id\":1}", "[" + KLINE + "]",
        KLINE.replace("\"x\":true", "\"x\":\"true\""),
        KLINE.replace("\"x\":true,", ""),
        KLINE.replace("\"n\":20", "\"n\":{\"x\":false}"),
        KLINE.replace("\"t\":0", "\"t\":[]"),
        KLINE + "{}",
        KLINE.substring(0, KLINE.length() - 1) + ",\"stream\":\"x\",\"data\":[]}")) {
      assertThat(BinanceKlineHeader.parse(raw, serializer)).as(raw).isNull();
    }
  }

  @Test
  void unrelatedNestedFieldsAndLiteralClosedTextCannotChangeTheHeader() {
    String raw = KLINE.replace("\"x\":true", "\"x\":false")
        .replace("\"n\":20", "\"unknown\":{\"x\":true,\"n\":999},\"text\":\"x=true\",\"n\":20");
    var header = BinanceKlineHeader.parse(raw, serializer);
    assertThat(header.closed()).isFalse();
    assertThat(header.tradeCount()).isEqualTo(20);
    assertThat(BinanceKlineHeader.parse(KLINE.replace("\"E\":100", "\"E\":\"invalid\""), serializer).closed()).isTrue();
  }

  @Test
  void continuousClassificationUsesOnlyPublishedRoutesAndNormalizesToTheOrdinarySymbol() {
    AtomicInteger exchangeQueries = new AtomicInteger();
    BinanceFutureSymbol symbol = new BinanceFutureSymbol();
    symbol.setSymbol("BTC_PERP"); symbol.setPair("BTCUSDT"); symbol.setContractType("PERPETUAL"); symbol.setStatus("TRADING");
    BinanceFutureExchange exchange = new BinanceFutureExchange(); exchange.setSymbols(List.of(symbol));
    var stream = new BinanceContinuousKlineStream(() -> { exchangeQueries.incrementAndGet(); return exchange; });
    var header = BinanceKlineHeader.parse(KLINE.replace("\"e\":\"kline\",\"s\":\"BTCUSDT\"",
        "\"e\":\"continuous_kline\",\"ps\":\"BTCUSDT\",\"ct\":\"PERPETUAL\""), serializer);
    assertThat(stream.dispatchMetadata("future", header)).isNull();
    assertThat(exchangeQueries.get()).isZero();
    stream.subscriptionState(); // prepare/refresh metadata outside the ingress callback
    var metadata = stream.dispatchMetadata("future", header);
    var ordinary = new BinanceKlineStream().dispatchMetadata("future",
        BinanceKlineHeader.parse(KLINE.replace("BTCUSDT", "BTC_PERP"), serializer));
    assertThat(metadata.series()).isEqualTo(ordinary.series());
    assertThat(metadata.topic()).isEqualTo("btcusdt_perpetual@continuousKline_1h");
    assertThat(exchangeQueries.get()).isEqualTo(1);
  }
}
