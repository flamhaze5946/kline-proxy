package com.zx.quant.klineproxy.service.stream;

import static org.junit.jupiter.api.Assertions.*;

import com.zx.quant.klineproxy.config.SerializeConfig;
import com.zx.quant.klineproxy.model.KlineDispatchMetadata;
import com.zx.quant.klineproxy.model.ParsedWebSocketMessage;
import com.zx.quant.klineproxy.model.enums.NumberTypeEnum;
import com.zx.quant.klineproxy.util.Serializer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DirectKlineDecodeTest {
  private final Serializer serializer = new Serializer(new SerializeConfig().objectMapper());
  private static final String FRAME = """
      {"e":"kline","E":1790708400105,"s":"BTCUSDT","k":{
        "t":1790704800000,"T":1790708399999,"s":"BTCUSDT","i":"1h","f":1,"L":100,
        "o":"100000.123456789123","h":"100100.125","l":"99999.12","c":"100050.000000019",
        "v":"0.000000019","q":"123456789.987654321","V":"0","Q":"0.00","n":100,"x":true,"B":"0"}}
      """;

  @Test
  void directRawAndCombinedDecodingMatchesTheTreeForEveryPrecisionAndStream() {
    for (NumberTypeEnum number : NumberTypeEnum.values()) {
      for (boolean continuous : List.of(false, true)) {
        String payload = continuous ? FRAME.replace("\"e\":\"kline\"",
            "\"e\":\"continuous_kline\",\"ps\":\"BTCUSDT\",\"ct\":\"PERPETUAL\"") : FRAME;
        for (boolean combined : List.of(false, true)) {
          String raw = combined ? "{\"data\":" + payload + ",\"stream\":\"topic\"}" : payload;
          var header = BinanceKlineHeader.parse(raw, serializer);
          assertNotNull(header);
          assertTrue(header.directDecodeSafe());
          var meta = new KlineDispatchMetadata(new KlineDispatchMetadata.Series("future", "BTCUSDT", "1h"),
              header.openTime(), header.closed(), header.tradeCount(), header.eventTime(), "topic",
              header.eventType(), header.stream());
          AtomicInteger trees = new AtomicInteger();
          var message = ParsedWebSocketMessage.classified(raw, null, 12, meta, () -> {
            trees.incrementAndGet(); return serializer.readTree(raw);
          });
          AbstractBinanceKlineStream stream = continuous
              ? new BinanceContinuousKlineStream(() -> { throw new AssertionError("Metadata must not be reloaded"); })
              : new BinanceKlineStream();
          var decoded = stream.parse(message, number, serializer);
          var expected = serializer.treeToValue(serializer.readTree(payload), number.eventKlineEventClass());
          assertEquals(expected, decoded, number + " combined=" + combined + " continuous=" + continuous);
          assertEquals(0, trees.get());
          assertSame(message.rootNode(), message.rootNode());
          assertEquals(serializer.readTree(payload), message.payloadNode());
          assertEquals(1, trees.get(), "Extension handlers can still read a single lazily materialized tree");
        }
      }
    }
  }

  @Test
  void nonstandardNumericAndAmbiguousPayloadsRetainTheLegacyCoercionPath() {
    for (String raw : List.of(
        FRAME.replace("\"v\":\"0.000000019\"", "\"v\":0.000000019"),
        FRAME.replace("\"E\":1790708400105", "\"E\":1.23e4"),
        FRAME.replace("\"e\":\"kline\"", "\"stream\":\"topic\",\"e\":\"kline\""),
        FRAME.replace("\"e\":\"kline\"", "\"stream\":\"topic\",\"data\":null,\"e\":\"kline\""),
        "{\"data\":" + FRAME + ",\"data\":" + FRAME + ",\"stream\":\"topic\"}",
        FRAME.replace("\"e\":\"kline\"", "\"id\":1,\"result\":[],\"e\":\"kline\""))) {
      var header = BinanceKlineHeader.parse(raw, serializer);
      assertNotNull(header);
      assertFalse(header.directDecodeSafe());
      assertNull(new BinanceKlineStream().dispatchMetadata("future", header).eventType());
    }
  }
}
