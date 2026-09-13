package com.zx.quant.klineproxy.service.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.zx.quant.klineproxy.model.EventKlineEvent;
import com.zx.quant.klineproxy.model.KlineDispatchMetadata;
import com.zx.quant.klineproxy.model.ParsedWebSocketMessage;
import com.zx.quant.klineproxy.model.enums.NumberTypeEnum;
import com.zx.quant.klineproxy.util.Serializer;
import org.apache.commons.lang3.StringUtils;

/** Stream protocol only; storage, finality waits and interval selection belong to the service. */
public abstract class AbstractBinanceKlineStream {

  public abstract String eventType();

  /** Null means this stream cannot represent the requested symbol; the service uses the ordinary stream. */
  public abstract String subscriptionTopic(String symbol, String interval);

  protected abstract String resolveSymbol(EventKlineEvent<?, ?> event);

  protected abstract String rawTopic(JsonNode payload);

  protected abstract String resolveSymbol(BinanceKlineHeader header);

  protected abstract String rawTopic(BinanceKlineHeader header);

  public final KlineDispatchMetadata dispatchMetadata(String market, BinanceKlineHeader header) {
    if (!accepts(header.eventType())) {
      return null;
    }
    String symbol = resolveSymbol(header);
    String topic = rawTopic(header);
    if (StringUtils.isAnyBlank(symbol, topic, header.interval())) {
      return null;
    }
    return new KlineDispatchMetadata(new KlineDispatchMetadata.Series(market, symbol, header.interval()),
        header.openTime(), header.closed(), header.tradeCount(), header.eventTime(),
        StringUtils.isNotBlank(header.stream()) ? header.stream() : topic);
  }

  public final boolean accepts(String eventType) {
    return eventType().equals(eventType);
  }

  public final EventKlineEvent<?, ?> parse(ParsedWebSocketMessage message,
                                         NumberTypeEnum numberType, Serializer serializer) {
    if (!accepts(message.eventType())) {
      return null;
    }
    EventKlineEvent<?, ?> event = serializer.treeToValue(message.payloadNode(), numberType.eventKlineEventClass());
    if (event == null || event.getEventKline() == null) {
      return null;
    }
    // Preserve the identity chosen before queueing even if exchange metadata refreshes meanwhile.
    String symbol = message.dispatchMetadata() != null ? message.dispatchMetadata().series().symbol()
        : resolveSymbol(event);
    if (StringUtils.isBlank(symbol)) {
      return null;
    }
    event.setSymbol(symbol);
    return event;
  }

  /** Use the incoming protocol, regardless of a configuration switch or an in-flight unsubscribe. */
  public final String extractTopic(ParsedWebSocketMessage message) {
    if (!accepts(message.eventType())) {
      return null;
    }
    String topic = rawTopic(message.payloadNode());
    if (topic == null) {
      return null;
    }
    return StringUtils.isNotBlank(message.stream()) ? message.stream() : topic;
  }

  /** Immutable metadata affecting subscription topics, used to invalidate the service's topic cache. */
  public Object subscriptionState() {
    return null;
  }

  protected static String text(JsonNode node, String field) {
    return node != null && node.isObject() ? node.path(field).asText(null) : null;
  }

  protected static String interval(JsonNode payload) {
    return text(payload != null ? payload.get("k") : null, "i");
  }
}
