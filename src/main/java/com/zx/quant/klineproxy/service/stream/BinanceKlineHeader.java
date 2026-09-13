package com.zx.quant.klineproxy.service.stream;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.zx.quant.klineproxy.util.Serializer;
import java.io.IOException;

/** Small streaming header scan; malformed/unknown frames fall back to the reliable generic path. */
public record BinanceKlineHeader(String eventType, String symbol, String pair, String contractType,
    String interval, long openTime, boolean closed, int tradeCount, Long eventTime, String stream) {

  public static BinanceKlineHeader parse(String raw, Serializer serializer) {
    try (JsonParser parser = serializer.createParser(raw)) {
      if (parser.nextToken() != JsonToken.START_OBJECT) {
        return null;
      }
      Fields root = readObject(parser, true);
      if (parser.nextToken() != null) {
        return null;
      }
      Fields payload = root.stream != null && !root.stream.isBlank() && root.hasPayload ? root.data : root;
      if (payload == null || !("kline".equals(payload.event) || "continuous_kline".equals(payload.event))
          || payload.kline == null || payload.kline.open == null || payload.kline.closed == null
          || payload.kline.trades == null || payload.kline.trades < 0 || payload.kline.interval == null) {
        return null;
      }
      return new BinanceKlineHeader(payload.event, payload.symbol, payload.pair, payload.contract,
          payload.kline.interval, payload.kline.open, payload.kline.closed, payload.kline.trades,
          payload.eventTime, root.stream);
    } catch (IOException | RuntimeException invalidHeader) {
      return null;
    }
  }

  private static Fields readObject(JsonParser parser, boolean root) throws IOException {
    Fields fields = new Fields();
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      if (parser.currentToken() != JsonToken.FIELD_NAME) {
        throw new IOException("Expected object field");
      }
      String name = parser.currentName();
      parser.nextToken();
      switch (name) {
        case "e" -> fields.event = text(parser);
        case "s" -> fields.symbol = text(parser);
        case "ps" -> fields.pair = text(parser);
        case "ct" -> fields.contract = text(parser);
        case "E" -> fields.eventTime = eventTime(parser);
        case "stream" -> fields.stream = text(parser);
        case "data" -> {
          fields.hasPayload = root && parser.currentToken() != JsonToken.VALUE_NULL;
          fields.data = root && parser.currentToken() == JsonToken.START_OBJECT ? readObject(parser, false) : null;
          parser.skipChildren();
        }
        case "k" -> {
          fields.kline = parser.currentToken() == JsonToken.START_OBJECT ? readKline(parser) : null;
          parser.skipChildren();
        }
        default -> parser.skipChildren();
      }
      parser.skipChildren();
    }
    return fields;
  }

  private static KlineFields readKline(JsonParser parser) throws IOException {
    KlineFields fields = new KlineFields();
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      if (parser.currentToken() != JsonToken.FIELD_NAME) {
        throw new IOException("Expected kline field");
      }
      String name = parser.currentName();
      parser.nextToken();
      switch (name) {
        case "i" -> fields.interval = text(parser);
        case "t" -> fields.open = parser.currentToken() == JsonToken.VALUE_NUMBER_INT ? parser.getLongValue() : null;
        case "n" -> fields.trades = parser.currentToken() == JsonToken.VALUE_NUMBER_INT ? parser.getIntValue() : null;
        case "x" -> fields.closed = parser.currentToken() == JsonToken.VALUE_TRUE ? Boolean.TRUE
            : parser.currentToken() == JsonToken.VALUE_FALSE ? Boolean.FALSE : null;
        default -> parser.skipChildren();
      }
      parser.skipChildren();
    }
    return fields;
  }

  private static String text(JsonParser parser) throws IOException {
    if (parser.currentToken() != null && parser.currentToken().isScalarValue()
        && parser.currentToken() != JsonToken.VALUE_NULL) {
      return parser.getValueAsString();
    }
    parser.skipChildren();
    return null;
  }

  private static Long eventTime(JsonParser parser) throws IOException {
    if (parser.currentToken() == JsonToken.VALUE_NUMBER_INT) {
      return parser.getLongValue();
    }
    String value = text(parser);
    try {
      return value == null ? null : Long.valueOf(value.trim());
    } catch (NumberFormatException ignored) {
      return null; // invalid E is diagnostic-only, never a reason to lose a candle
    }
  }

  private static final class Fields {
    private String event, symbol, pair, contract, stream;
    private Long eventTime;
    private Fields data;
    private boolean hasPayload;
    private KlineFields kline;
  }

  private static final class KlineFields {
    private String interval;
    private Long open;
    private Boolean closed;
    private Integer trades;
  }
}
